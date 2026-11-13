package uz.businesshub.gateway.error;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.codec.HttpMessageWriter;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.reactive.result.view.ViewResolver;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import uz.businesshub.gateway.filter.CorrelationIdFilter;

import java.net.ConnectException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeoutException;

@Component
@Order(-2)
public class GatewayErrorHandler implements org.springframework.boot.webflux.error.ErrorWebExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GatewayErrorHandler.class);
    private static final String ERROR_BASE = "https://businesshub.uz/errors/";

    private final List<HttpMessageWriter<?>> writers;

    public GatewayErrorHandler(ServerCodecConfigurer codecs) {
        this.writers = codecs.getWriters();
    }

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable ex) {
        if (exchange.getResponse().isCommitted()) {
            return Mono.error(ex);
        }

        String correlationId = exchange.getResponse().getHeaders().getFirst(CorrelationIdFilter.HEADER);
        ProblemDetail problem = toProblem(ex);
        problem.setInstance(URI.create(exchange.getRequest().getPath().value()));
        problem.setProperty("correlationId", correlationId);

        if (problem.getStatus() >= 500) {
            log.warn("Gateway error {} on {} correlationId={}: {}",
                    problem.getStatus(), exchange.getRequest().getPath(), correlationId, ex.toString());
        }

        return ServerResponse.status(problem.getStatus())
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .bodyValue(problem)
                .flatMap(response -> response.writeTo(exchange, new ServerResponse.Context() {
                    @Override public List<HttpMessageWriter<?>> messageWriters() { return writers; }
                    @Override public List<ViewResolver> viewResolvers() { return List.of(); }
                }));
    }

    private ProblemDetail toProblem(Throwable ex) {
        if (hasCause(ex, ConnectException.class)) {
            return problem(HttpStatus.SERVICE_UNAVAILABLE, "Service unavailable",
                    "Downstream service is not reachable", "SERVICE_UNAVAILABLE");
        }
        if (hasCause(ex, TimeoutException.class)) {
            return problem(HttpStatus.GATEWAY_TIMEOUT, "Gateway timeout",
                    "Downstream service did not respond in time", "GATEWAY_TIMEOUT");
        }
        if (ex instanceof ResponseStatusException rse) {
            HttpStatusCode status = rse.getStatusCode();
            if (status.value() == 404) {
                return problem(HttpStatus.NOT_FOUND, "Not found", "No route for this path", "ROUTE_NOT_FOUND");
            }
            return problem(HttpStatus.valueOf(status.value()), "Gateway error",
                    rse.getReason() != null ? rse.getReason() : "Request failed", "HTTP_" + status.value());
        }
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Internal error",
                "Unexpected gateway error", "INTERNAL_ERROR");
    }

    private static boolean hasCause(Throwable ex, Class<? extends Throwable> type) {
        Throwable current = ex;
        for (int depth = 0; current != null && depth < 10; depth++) {
            if (type.isInstance(current)) return true;
            current = current.getCause();
        }
        return false;
    }

    private static ProblemDetail problem(HttpStatus status, String title, String detail, String code) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(status, detail);
        pd.setType(URI.create(ERROR_BASE + code.toLowerCase().replace('_', '-')));
        pd.setTitle(title);
        pd.setProperty("code", code);
        return pd;
    }
}