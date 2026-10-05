"""
Group pane, input, and layout owners composed by the terminal's curses driver.

Child modules share state contracts and lightweight models without importing the
public driver composition. This initializer performs no curses setup and eagerly
imports no pane implementations.
"""
