import requests

class WeatherApiClient:

    def __init__(self, api_key: str):
        self.base_url = "http://api.openweathermap.org/data/2.5"
        if api_key is None: 
            raise Exception("API key cannot be set to None.")
        self.api_key = api_key
    
    def get_city(self, city_name: str, temperature_units: str="metric") -> dict:
        """
        Get the latest weather data for a specified city. 

        Args: 
            city_name: the name of the city in english. Refer to ISO 3166 for city name or country codes: https://www.iso.org/obp/ui/#search
            temperature_units: temperature units, supports `standard`, `metric` and `imperial`. 
        
        Returns: 
            City temperature dictionary 
        
        Raises:
            Exception if response code is not 200. 
        """
        params = {
            "q": city_name,
            "units": temperature_units,
            "appid": self.api_key
        }
        response = requests.get(f"{self.base_url}/weather", params=params)
        if response.status_code == 200: 
            return response.json()
        else: 
            raise Exception(f"Failed to extract data from Open Weather API. Status Code: {response.status_code}. Response: {response.text}")
