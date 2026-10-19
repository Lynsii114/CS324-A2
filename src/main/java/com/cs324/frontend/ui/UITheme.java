package com.cs324.frontend.ui;

import atlantafx.base.theme.PrimerLight;
import atlantafx.base.theme.Theme;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.paint.Color;

/**
 * Shared JavaFX look-and-feel for the client GUI.
 *
 * <p>The visual foundation is <b>AtlantaFX</b> (a modern, CSS-driven theme for
 * JavaFX, loaded here as the light "Primer" theme). On top of that we layer a
 * small custom stylesheet ({@link #STYLESHEET}) that defines the project's
 * palette, generously-sized typography and reusable widget classes
 * ({@code card}, {@code pill}, {@code pill-*}, {@code event-*}, ...). This
 * keeps the two libraries combined in one consistent, modern UI.</p>
 *
 * <p>Fonts are deliberately larger than stock JavaFX defaults so the dashboard
 * is readable at a glance.</p>
 */
public final class UITheme {

    /** The base AtlantaFX theme (modern light theme). */
    public static final Theme BASE_THEME = new PrimerLight();

    private UITheme() {
    }

    /** Applies the AtlantaFX base theme as the application user-agent stylesheet. */
    public static void install() {
        javafx.application.Application.setUserAgentStylesheet(BASE_THEME.getUserAgentStylesheet());
    }

    /**
     * Returns the stylesheets to attach to the scene: the AtlantaFX base
     * theme first, then the project stylesheet
     * ({@code client-theme.css}) so both libraries combine into one look.
     */
    public static String[] stylesheets() {
        java.net.URL project = UITheme.class.getResource("/com/cs324/frontend/ui/client-theme.css");
        if (project == null) {
            return new String[]{BASE_THEME.getUserAgentStylesheet()};
        }
        return new String[]{BASE_THEME.getUserAgentStylesheet(), project.toExternalForm()};
    }

    /**
     * Convenience: attach both the AtlantaFX theme and the project stylesheet
     * to a scene, layered in that order.
     */
    public static void apply(javafx.scene.Scene scene) {
        for (String sheet : stylesheets()) {
            if (!scene.getStylesheets().contains(sheet)) {
                scene.getStylesheets().add(sheet);
            }
        }
    }

    // ---------------------------------------------------------------- palette

    public static final Color ACCENT = Color.web("#2F6FED");
    public static final Color SUCCESS = Color.web("#1A7F37");
    public static final Color WARNING = Color.web("#9A6700");
    public static final Color DANGER = Color.web("#CF222E");
    public static final Color MUTED = Color.web("#5A6474");
    public static final Color TEXT = Color.web("#1F2328");

    // ------------------------------------------------------------- styling API

    /** Applies a status pill style ({@code pill-*}) to a label, clearing any previous one. */
    public static void setPillState(Label label, PillState state) {
        label.getStyleClass().removeAll("pill-idle", "pill-accent", "pill-success",
                "pill-warning", "pill-danger");
        label.getStyleClass().add(state.styleClass);
    }

    /** The set of visual states a dashboard pill can be in. */
    public enum PillState {
        IDLE("pill-idle"),
        ACCENT("pill-accent"),
        SUCCESS("pill-success"),
        WARNING("pill-warning"),
        DANGER("pill-danger");

        public final String styleClass;

        PillState(String styleClass) {
            this.styleClass = styleClass;
        }
    }

    /** Convenience: apply a style class to a node (idempotent). */
    public static void addStyle(Node node, String styleClass) {
        if (!node.getStyleClass().contains(styleClass)) {
            node.getStyleClass().add(styleClass);
        }
    }

    /** Colour palette shared by the dashboard widgets (mirrors client-theme.css). */
    public static final String CSS_SUCCESS = "text-ok";
    public static final String CSS_WARNING = "text-warn";
    public static final String CSS_DANGER = "text-bad";
}
