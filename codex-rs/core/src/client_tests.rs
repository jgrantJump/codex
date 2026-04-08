use super::AuthRequestTelemetryContext;
use super::ModelClient;
use super::ModelClientSession;
use super::PendingUnauthorizedRetry;
use super::Prompt;
use super::ResponseEvent;
use super::ResponseStream;
use super::UnauthorizedRecoveryExecution;
use super::X_CODEX_INSTALLATION_ID_HEADER;
use super::X_CODEX_PARENT_THREAD_ID_HEADER;
use super::X_CODEX_TURN_METADATA_HEADER;
use super::X_CODEX_WINDOW_ID_HEADER;
use super::X_OPENAI_SUBAGENT_HEADER;
use codex_api::ApiError;
use codex_api::CoreAuthProvider;
use codex_api::ResponsesApiRequest;
use codex_api::TransportError;
use codex_app_server_protocol::AuthMode;
use codex_model_provider_info::ModelProviderInfo;
use codex_model_provider_info::WireApi;
use codex_model_provider_info::create_oss_provider_with_base_url;
use codex_otel::SessionTelemetry;
use codex_protocol::ThreadId;
use codex_protocol::config_types::ReasoningSummary;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ReasoningItemContent;
use codex_protocol::models::ReasoningItemReasoningSummary;
use codex_protocol::models::ResponseItem;
use codex_protocol::openai_models::ModelInfo;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_response_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::start_websocket_server;
use core_test_support::skip_if_no_network;
use futures::StreamExt;
use http::StatusCode;
use pretty_assertions::assert_eq;
use serde_json::json;
use wiremock::MockServer;
use wiremock::ResponseTemplate;

fn test_model_client(session_source: SessionSource) -> ModelClient {
    test_model_client_with_provider(
        test_provider("https://example.com/v1", /*supports_websockets*/ false),
        session_source,
    )
}

fn test_model_client_with_provider(
    provider: ModelProviderInfo,
    session_source: SessionSource,
) -> ModelClient {
    ModelClient::new(
        /*auth_manager*/ None,
        ThreadId::new(),
        /*installation_id*/ "11111111-1111-4111-8111-111111111111".to_string(),
        provider,
        session_source,
        /*model_verbosity*/ None,
        /*enable_request_compression*/ false,
        /*include_timing_metrics*/ false,
        /*beta_features_header*/ None,
    )
}

fn test_provider(base_url: &str, supports_websockets: bool) -> ModelProviderInfo {
    let mut provider = create_oss_provider_with_base_url(base_url, WireApi::Responses);
    provider.request_max_retries = Some(0);
    provider.stream_max_retries = Some(0);
    provider.stream_idle_timeout_ms = Some(5_000);
    provider.supports_websockets = supports_websockets;
    provider
}

fn test_model_info() -> ModelInfo {
    serde_json::from_value(json!({
        "slug": "gpt-test",
        "display_name": "gpt-test",
        "description": "desc",
        "default_reasoning_level": "medium",
        "supported_reasoning_levels": [
            {"effort": "medium", "description": "medium"}
        ],
        "shell_type": "shell_command",
        "visibility": "list",
        "supported_in_api": true,
        "priority": 1,
        "upgrade": null,
        "base_instructions": "base instructions",
        "model_messages": null,
        "supports_reasoning_summaries": false,
        "support_verbosity": false,
        "default_verbosity": null,
        "apply_patch_tool_type": null,
        "truncation_policy": {"mode": "bytes", "limit": 10000},
        "supports_parallel_tool_calls": false,
        "supports_image_detail_original": false,
        "context_window": 272000,
        "auto_compact_token_limit": null,
        "experimental_supported_tools": []
    }))
    .expect("deserialize test model info")
}

fn test_session_telemetry() -> SessionTelemetry {
    SessionTelemetry::new(
        ThreadId::new(),
        "gpt-test",
        "gpt-test",
        /*account_id*/ None,
        /*account_email*/ None,
        /*auth_mode*/ None,
        "test-originator".to_string(),
        /*log_user_prompts*/ false,
        "test-terminal".to_string(),
        SessionSource::Cli,
    )
}

fn user_message(text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: "user".to_string(),
        content: vec![ContentItem::InputText {
            text: text.to_string(),
        }],
        end_turn: None,
        phase: None,
    }
}

fn turn_aborted_message() -> ResponseItem {
    user_message("<turn_aborted>\ninterrupted\n</turn_aborted>")
}

fn reasoning_item() -> ResponseItem {
    ResponseItem::Reasoning {
        id: "rs_123".to_string(),
        summary: vec![ReasoningItemReasoningSummary::SummaryText {
            text: "summary".to_string(),
        }],
        content: Some(vec![ReasoningItemContent::ReasoningText {
            text: "thinking".to_string(),
        }]),
        encrypted_content: None,
    }
}

fn request_with_orphan_reasoning() -> ResponsesApiRequest {
    ResponsesApiRequest {
        model: "gpt-test".to_string(),
        instructions: "base instructions".to_string(),
        input: vec![
            reasoning_item(),
            turn_aborted_message(),
            user_message("resume"),
        ],
        tools: Vec::new(),
        tool_choice: "auto".to_string(),
        parallel_tool_calls: false,
        reasoning: None,
        store: false,
        stream: true,
        include: Vec::new(),
        service_tier: None,
        prompt_cache_key: None,
        text: None,
        client_metadata: None,
    }
}

fn prompt_with_orphan_reasoning() -> Prompt {
    Prompt {
        input: request_with_orphan_reasoning().input,
        ..Default::default()
    }
}

async fn drain_response_stream(stream: &mut ResponseStream) -> anyhow::Result<()> {
    while let Some(event) = stream.next().await {
        if matches!(event?, ResponseEvent::Completed { .. }) {
            break;
        }
    }

    Ok(())
}

#[test]
fn build_subagent_headers_sets_other_subagent_label() {
    let client = test_model_client(SessionSource::SubAgent(SubAgentSource::Other(
        "memory_consolidation".to_string(),
    )));
    let headers = client.build_subagent_headers();
    let value = headers
        .get(X_OPENAI_SUBAGENT_HEADER)
        .and_then(|value| value.to_str().ok());
    assert_eq!(value, Some("memory_consolidation"));
}

#[test]
fn build_ws_client_metadata_includes_window_lineage_and_turn_metadata() {
    let parent_thread_id = ThreadId::new();
    let client = test_model_client(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 2,
        agent_path: None,
        agent_nickname: None,
        agent_role: None,
    }));

    client.advance_window_generation();

    let client_metadata = client.build_ws_client_metadata(Some(r#"{"turn_id":"turn-123"}"#));
    let conversation_id = client.state.conversation_id;
    assert_eq!(
        client_metadata,
        std::collections::HashMap::from([
            (
                X_CODEX_INSTALLATION_ID_HEADER.to_string(),
                "11111111-1111-4111-8111-111111111111".to_string(),
            ),
            (
                X_CODEX_WINDOW_ID_HEADER.to_string(),
                format!("{conversation_id}:1"),
            ),
            (
                X_OPENAI_SUBAGENT_HEADER.to_string(),
                "collab_spawn".to_string(),
            ),
            (
                X_CODEX_PARENT_THREAD_ID_HEADER.to_string(),
                parent_thread_id.to_string(),
            ),
            (
                X_CODEX_TURN_METADATA_HEADER.to_string(),
                r#"{"turn_id":"turn-123"}"#.to_string(),
            ),
        ])
    );
}

#[tokio::test]
async fn summarize_memories_returns_empty_for_empty_input() {
    let client = test_model_client(SessionSource::Cli);
    let model_info = test_model_info();
    let session_telemetry = test_session_telemetry();

    let output = client
        .summarize_memories(
            Vec::new(),
            &model_info,
            /*effort*/ None,
            &session_telemetry,
        )
        .await
        .expect("empty summarize request should succeed");
    assert_eq!(output.len(), 0);
}

#[test]
fn auth_request_telemetry_context_tracks_attached_auth_and_retry_phase() {
    let auth_context = AuthRequestTelemetryContext::new(
        Some(AuthMode::Chatgpt),
        &CoreAuthProvider::for_test(Some("access-token"), Some("workspace-123")),
        PendingUnauthorizedRetry::from_recovery(UnauthorizedRecoveryExecution {
            mode: "managed",
            phase: "refresh_token",
        }),
    );

    assert_eq!(auth_context.auth_mode, Some("Chatgpt"));
    assert!(auth_context.auth_header_attached);
    assert_eq!(auth_context.auth_header_name, Some("authorization"));
    assert!(auth_context.retry_after_unauthorized);
    assert_eq!(auth_context.recovery_mode, Some("managed"));
    assert_eq!(auth_context.recovery_phase, Some("refresh_token"));
}

#[test]
fn repaired_request_after_missing_reasoning_http_error_drops_orphan_reasoning() {
    let request = request_with_orphan_reasoning();
    let repaired = ModelClientSession::repaired_request_after_missing_reasoning_error(
        &ApiError::Transport(TransportError::Http {
            status: StatusCode::BAD_REQUEST,
            url: None,
            headers: None,
            body: Some(
                r#"{"error":{"message":"Item 'rs_123' of type 'reasoning' was provided without its required following item.","type":"invalid_request_error"}}"#
                    .to_string(),
            ),
        }),
        &request,
    )
    .expect("expected repaired request");

    assert_eq!(
        repaired.input,
        vec![turn_aborted_message(), user_message("resume")]
    );
}

#[test]
fn repaired_request_after_missing_reasoning_invalid_request_error_drops_orphan_reasoning() {
    let request = request_with_orphan_reasoning();
    let repaired = ModelClientSession::repaired_request_after_missing_reasoning_error(
        &ApiError::InvalidRequest {
            message:
                "Item 'rs_123' of type 'reasoning' was provided without its required following item."
                    .to_string(),
        },
        &request,
    )
    .expect("expected repaired request");

    assert_eq!(
        repaired.input,
        vec![turn_aborted_message(), user_message("resume")]
    );
}

#[test]
fn repaired_request_after_missing_reasoning_error_ignores_unrelated_invalid_request() {
    let request = request_with_orphan_reasoning();
    let repaired = ModelClientSession::repaired_request_after_missing_reasoning_error(
        &ApiError::InvalidRequest {
            message: "Model does not support image inputs.".to_string(),
        },
        &request,
    );

    assert_eq!(repaired, None);
}

#[tokio::test]
async fn http_stream_retries_after_missing_reasoning_following_item_error() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));

    let server = MockServer::start().await;
    let response_mock = mount_response_sequence(
        &server,
        vec![
            ResponseTemplate::new(400).set_body_json(json!({
                "error": {
                    "message": "Item 'rs_123' of type 'reasoning' was provided without its required following item.",
                    "type": "invalid_request_error",
                }
            })),
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(sse(vec![
                    ev_response_created("resp-1"),
                    ev_completed("resp-1"),
                ])),
        ],
    )
    .await;

    let client = test_model_client_with_provider(
        test_provider(
            &format!("{}/v1", server.uri()),
            /*supports_websockets*/ false,
        ),
        SessionSource::Cli,
    );
    let model_info = test_model_info();
    let session_telemetry = test_session_telemetry();
    let mut client_session = client.new_session();
    let prompt = prompt_with_orphan_reasoning();

    let mut stream = client_session
        .stream(
            &prompt,
            &model_info,
            &session_telemetry,
            /*effort*/ None,
            ReasoningSummary::Auto,
            /*service_tier*/ None,
            /*turn_metadata_header*/ None,
        )
        .await?;
    drain_response_stream(&mut stream).await?;

    let requests = response_mock.requests();
    assert_eq!(requests.len(), 2, "expected one retry with repaired input");
    assert_eq!(requests[0].inputs_of_type("reasoning").len(), 1);
    assert_eq!(requests[1].inputs_of_type("reasoning").len(), 0);
    assert!(
        requests[0]
            .message_input_texts("user")
            .iter()
            .any(|text| text.contains("<turn_aborted>"))
    );
    assert!(
        requests[1]
            .message_input_texts("user")
            .iter()
            .any(|text| text.contains("<turn_aborted>"))
    );
    assert!(
        requests[1]
            .message_input_texts("user")
            .iter()
            .any(|text| text == "resume")
    );

    Ok(())
}

#[tokio::test]
async fn websocket_stream_retries_after_missing_reasoning_following_item_error()
-> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_websocket_server(vec![
        vec![vec![json!({
            "type": "error",
            "status": 400,
            "error": {
                "type": "invalid_request_error",
                "message": "Item 'rs_123' of type 'reasoning' was provided without its required following item.",
            }
        })]],
        vec![vec![ev_response_created("resp-1"), ev_completed("resp-1")]],
    ])
    .await;

    let client = test_model_client_with_provider(
        test_provider(
            &format!("{}/v1", server.uri()),
            /*supports_websockets*/ true,
        ),
        SessionSource::Cli,
    );
    let model_info = test_model_info();
    let session_telemetry = test_session_telemetry();
    let mut client_session = client.new_session();
    let prompt = prompt_with_orphan_reasoning();

    let mut stream = client_session
        .stream(
            &prompt,
            &model_info,
            &session_telemetry,
            /*effort*/ None,
            ReasoningSummary::Auto,
            /*service_tier*/ None,
            /*turn_metadata_header*/ None,
        )
        .await?;
    drain_response_stream(&mut stream).await?;

    let connections = server.connections();
    assert_eq!(
        connections.len(),
        2,
        "expected websocket retry to reconnect after the wrapped 400"
    );

    let request_bodies: Vec<_> = connections
        .into_iter()
        .flatten()
        .map(|request| request.body_json())
        .collect();
    assert_eq!(request_bodies.len(), 2);
    assert_eq!(
        request_bodies[0]["input"]
            .as_array()
            .expect("missing first websocket input")
            .iter()
            .filter(|item| item["type"] == "reasoning")
            .count(),
        1
    );
    assert_eq!(
        request_bodies[1]["input"]
            .as_array()
            .expect("missing second websocket input")
            .iter()
            .filter(|item| item["type"] == "reasoning")
            .count(),
        0
    );
    assert!(request_bodies[1].to_string().contains("<turn_aborted>"));
    assert!(request_bodies[1].to_string().contains("resume"));

    server.shutdown().await;
    Ok(())
}
