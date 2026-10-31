package dev.vortex.api.market;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

/**
 * Tick sizes from config/symbols.yaml, the single source of truth for units (ADR 0004).
 * Converting ticks to a price happens only here, for presentation, with BigDecimal: never a float.
 */
@Component
public class SymbolRegistry {

    private final Map<String, BigDecimal> tickSize = new LinkedHashMap<>();

    public SymbolRegistry(@Value("${vortex.symbols-file}") String symbolsFile) {
        try (InputStream in = Files.newInputStream(Path.of(symbolsFile))) {
            Map<String, Object> root = new Yaml().load(in);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> symbols = (List<Map<String, Object>>) root.get("symbols");
            for (Map<String, Object> entry : symbols) {
                // tick_size is a string in the file, so BigDecimal gets it exactly ("0.01")
                tickSize.put((String) entry.get("symbol"), new BigDecimal((String) entry.get("tick_size")));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("cannot read " + symbolsFile, e);
        }
    }

    public boolean isKnown(String symbol) {
        return tickSize.containsKey(symbol);
    }

    public Set<String> symbols() {
        return tickSize.keySet();
    }

    /** 19005 ticks of AAPL -> "190.05", exact. */
    public String price(String symbol, long ticks) {
        return tickSize.get(symbol).multiply(BigDecimal.valueOf(ticks)).toPlainString();
    }
}
