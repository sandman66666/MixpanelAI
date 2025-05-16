"""
Anthropic Configuration Module

This module contains configuration settings for the Anthropic Claude integration.
"""

import os
from typing import Dict, Any, Optional

# Default Anthropic settings
DEFAULT_MODEL = "claude-3-opus-20240229"
DEFAULT_SYSTEM_PROMPT = """
You are Claude, an AI assistant specialized in analyzing Mixpanel analytics data for the HitCraft platform.

HitCraft is a creative AI platform that helps musicians with music production, lyrics/composition, and more.

Your task is to answer questions about platform performance, user behavior, and metrics using the analytics data provided.
Always support your answers with actual data and metrics when available.

When answering questions:
1. Focus on providing clear, actionable insights rather than general statements
2. Support your analysis with specific metrics and trends
3. Highlight significant changes or patterns
4. Provide recommendations when appropriate
5. Be honest about data limitations when information is not available

For data visualizations, describe the type of visualization that would be most helpful.
"""

class AnthropicConfig:
    """
    Configuration settings for Anthropic Claude integration.
    
    Manages API keys, model selection, and other parameters.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Anthropic configuration.
        
        Args:
            api_key: Optional API key for Anthropic. If not provided, will use environment variable.
        """
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        self.model = os.getenv("ANTHROPIC_MODEL", DEFAULT_MODEL)
        
        # Default parameters for API calls
        self.default_params = {
            "temperature": float(os.getenv("ANTHROPIC_TEMPERATURE", "0.2")),
            "max_tokens": int(os.getenv("ANTHROPIC_MAX_TOKENS", "2000")),
            "top_p": float(os.getenv("ANTHROPIC_TOP_P", "0.95")),
            "top_k": int(os.getenv("ANTHROPIC_TOP_K", "50"))
        }
        
        # System prompt for analytics assistant
        self.system_prompt = os.getenv("ANTHROPIC_SYSTEM_PROMPT", DEFAULT_SYSTEM_PROMPT)
    
    def get_api_key(self) -> Optional[str]:
        """
        Get the Anthropic API key.
        
        Returns:
            Optional[str]: The API key if available
        """
        return self.api_key
    
    def get_model(self) -> str:
        """
        Get the Anthropic model to use.
        
        Returns:
            str: The model name
        """
        return self.model
    
    def get_api_params(self, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get parameters for Anthropic API calls.
        
        Args:
            overrides: Optional parameter overrides
            
        Returns:
            Dict[str, Any]: Parameters for API calls
        """
        params = self.default_params.copy()
        
        if overrides:
            params.update(overrides)
            
        return params
    
    def get_system_prompt(self) -> str:
        """
        Get the system prompt for the analytics assistant.
        
        Returns:
            str: The system prompt
        """
        return self.system_prompt
    
    def is_configured(self) -> bool:
        """
        Check if the Anthropic integration is properly configured.
        
        Returns:
            bool: True if API key is available
        """
        return bool(self.api_key)


# Singleton instance
_config = None

def get_anthropic_config() -> AnthropicConfig:
    """
    Get the Anthropic configuration instance.
    
    Returns:
        AnthropicConfig: The configuration instance
    """
    global _config
    
    if _config is None:
        _config = AnthropicConfig()
        
    return _config
