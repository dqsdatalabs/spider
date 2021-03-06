# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import json
import math

from scrapy import Spider, Request, FormRequest
from python_spiders.loaders import ListingLoader
from python_spiders.user_agents import random_user_agent

from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Uk_immobilien_comSpider(Spider):
    name = 'uk_immobilien_com'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.uk-simmobilien.com"]
    position = 1
    custom_settings = {
        "User-Agent": random_user_agent()

    }
    start_urls = ["https://www.uk-immobilien.com/immobilienangebote/"]

    headers = {
        "POST": "/wp-admin/admin-ajax.php HTTP/1.1",
        "Host": "www.uk-immobilien.com",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Length": "231",
        "Origin": "https://www.uk-immobilien.com",
        "Connection": "keep-alive",
        "Referer": "https://www.uk-immobilien.com/immobilienangebote/",
        "Cookie": "borlabs-cookie=%7B%22consents%22%3A%7B%22essential%22%3A%5B%22borlabs-cookie%22%5D%2C%22statistics%22%3A%5B%22google-analytics%22%5D%2C%22marketing%22%3A%5B%22facebook-pixel%22%5D%2C%22external-media%22%3A%5B%22facebook%22%2C%22googlemaps%22%2C%22instagram%22%2C%22openstreetmap%22%2C%22twitter%22%2C%22vimeo%22%2C%22youtube%22%5D%7D%2C%22domainPath%22%3A%22www.uk-immobilien.com%2F%22%2C%22expires%22%3A%22Wed%2C%2028%20Dec%202022%2012%3A56%3A38%20GMT%22%2C%22uid%22%3A%22o0ym5ob1-dv9grhym-hdho5gdp-2kb2myl0%22%2C%22version%22%3A%221%22%7D; _ga_L5K88ZG936=GS1.1.1640696208.1.1.1640696303.0; _ga=GA1.2.26648207.1640696215; _fbp=fb.1.1640696221479.363460986; _gid=GA1.2.681883350.1640696222; _gat_gtag_UA_188975355_1=1",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin"
    }

    body = {
            "action": "wlac/ajax/fetch-units",
            "params[Vermarktungsart]": "miete",
            "params[sorting][value]": "Ge??ndert am",
            "params[sorting][order]": "high to low",
            "params[Language]": "DEU",
            "params[Post status]": "publish"
            }

    def parse(self, response):
        script_params = response.css("script:contains('wlacPublic')::text").get()
        body_to_add = json.loads(script_params.split("=")[1].split(";")[0])
        self.body["nonce"] = body_to_add["nonce"]
        
        yield FormRequest(url=f"https://www.uk-immobilien.com/wp-admin/admin-ajax.php",
                    callback=self.get_properties,
                    formdata = self.body,
                    headers = self.headers,
                    dont_filter = True,
                    method='POST')

    def get_properties(self, response):
        parsed_response_properties = json.loads(response.body)
        for property_data in parsed_response_properties["data"]:
            if(property_data["Verkauft"] == "0" and property_data["Reserviert"] == "0"):
                yield Request(response.urljoin(property_data["Permalink"]), callback=self.populate_item, meta = {"property_data": property_data}, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"
        property_data = response.meta.get("property_data")

        external_id = response.css("div.wlac-title-id:contains('Objekt ID:')::text").get()
        external_id = external_id.split("ID:")[1]

        title = response.css("div.block-title h1::text").get()
        lowered_title = title.lower()
        if(
            "gewerbe" in lowered_title
            or "gewerbefl??che" in lowered_title
            or "b??ro" in lowered_title
            or "praxisfl??chen" in lowered_title
            or "ladenlokal" in lowered_title
            or "arbeiten" in lowered_title 
            or "gewerbeeinheit" in lowered_title
            or "vermietet" in lowered_title
            or "stellplatz" in lowered_title
            or "garage" in lowered_title
            or "restaurant" in lowered_title
            or "lager" in lowered_title
            or "einzelhandel" in lowered_title
            or "sonstige" in lowered_title
            or "grundst??ck" in lowered_title
        ):
            return

        cold_rent = response.css("div.wlac-field-group-label:contains('Kaltmiete') + div.wlac-field-group-value::text").get()
        warm_rent = response.css("div.wlac-field-group-label:contains('Warmmiete') + div.wlac-field-group-value::text").get()

        rent = None
        if( not cold_rent ):
            cold_rent = "0"
        
        if( not warm_rent):
            warm_rent = "0"

        cold_rent = re.findall(r"([0-9]+)", cold_rent)
        cold_rent = "".join(cold_rent)

        warm_rent = re.findall(r"([0-9]+)", warm_rent)
        warm_rent = "".join(warm_rent)
        
        cold_rent = int(cold_rent)
        warm_rent = int (warm_rent)
        if(warm_rent > cold_rent):
            rent = str(warm_rent)
        else: 
            rent = str(cold_rent)
        
        if(not rent):
            return
        
        currency = "EUR"

        square_meters = response.css("div.wlac-field-group-label:contains('Wohnfl??che') + div.wlac-field-group-value::text").get()
        if(square_meters):
            square_meters = re.findall(r"([0-9]+)", square_meters)
            square_meters = ".".join(square_meters)
            square_meters = str(math.ceil(float(square_meters)))

        room_count = response.css("div.wlac-field-group-label:contains('Zimmeranzahl') + div.wlac-field-group-value::text").get()
        bathroom_count = response.css("div.wlac-field-group-label:contains('Badezimmer') + div.wlac-field-group-value::text").get()        

        latitude = property_data["Breitengrad"]
        longitude = property_data["L??ngengrad"]

        location_data = extract_location_from_coordinates(longitude, latitude)
        address = location_data[2]
        city = location_data[1]
        zipcode = location_data[0]

        images = response.css("div.splide__slide img::attr(data-splide-lazy)").getall()

        description = response.css("div.block-description:contains('Beschreibung')::text").getall()
        description = " ".join(description)
        description = description_cleaner(description)

        floor = response.css("div.wlac-field-group-label:contains('Etage') + div.wlac-field-group-value::text").get()

        energy_label = response.css("div.wlac-field-group-label:contains('Energieeffizienzklasse') + div.wlac-field-group-value::text").get()

        features = response.css("div.block-austattung::text").getall()
        features = " ".join(features).lower()

        balcony = "balkon" in features
        terrace = "terrasse" in features
        elevator = "aufzug" in features

        utilities = response.css("div.wlac-field-group-label:contains('Nebenkosten') + div.wlac-field-group-value::text").get()
        utilities = utilities.split(",")[0]

        deposit = response.css("div.wlac-field-group-label:contains('Kaution') + div.wlac-field-group-value::text").get()
        available_date = response.css("div.wlac-field-group-label:contains('Verf??gbar ab') + div.wlac-field-group-value::text").get()
        if(available_date):
            available_date = available_date.strip()

        landlord_name = "uk-immobilien"
        landlord_phone = "069 54 44 77"
        landlord_email = "info@uk-immobilien.com"

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url) 
        item_loader.add_value("external_source", self.external_source) 

        item_loader.add_value("external_id", external_id) 
        item_loader.add_value("position", self.position) 
        self.position += 1
        item_loader.add_value("title", title) 
        item_loader.add_value("description", description) 

        item_loader.add_value("city", city) 
        item_loader.add_value("zipcode", zipcode) 
        item_loader.add_value("address", address) 
        item_loader.add_value("latitude", latitude) 
        item_loader.add_value("longitude", longitude) 
        item_loader.add_value("floor", floor) 
        item_loader.add_value("property_type", property_type)  
        item_loader.add_value("square_meters", square_meters) 
        item_loader.add_value("room_count", room_count) 
        item_loader.add_value("bathroom_count", bathroom_count) 

        item_loader.add_value("available_date", available_date) 

        item_loader.add_value("elevator", elevator) 
        item_loader.add_value("balcony", balcony) 
        item_loader.add_value("terrace", terrace) 

        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 

        item_loader.add_value("rent_string", rent) 
        item_loader.add_value("deposit", deposit) 

        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("energy_label", energy_label) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 
        item_loader.add_value("landlord_email", landlord_email) 

        yield item_loader.load_item()
