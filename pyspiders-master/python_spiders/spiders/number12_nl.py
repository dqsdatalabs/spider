# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'number12_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    headers = {
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
            "origin": "https://www.number12.nl"
        }
    def start_requests(self):
        for i in range(0,48,12):
            data = {
                 "action": "all_houses",
                  "api": "2aefcc9c36fa3fead3526ca0b5ccb910",
                  "filter": "status=rent",
                  "offsetRow":f"{i}",
                  "numberRows":"12",
                  "path":"/woningaanbod?status=rent",
           }
           
            yield FormRequest(
                "https://cdn.eazlee.com/eazlee/api/query_functions.php",
                formdata=data,
                headers=self.headers,
                callback=self.parse,
            )
    
    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        
        if data:
            for item in data:
                follow_url = "https://www.number12.nl/woning?s-Gravenhage/" + item["street"].replace(" ", "-") + "/" + item["house_id"]
                external_id = item["house_id"]
                prop_type = item["house_type"]
                
                data = {
                "action": "property",
                "url": follow_url,
                "api": "2aefcc9c36fa3fead3526ca0b5ccb910",
                "path":"/woning",
            }
                if prop_type:
                    if "Appartement" in prop_type:
                        property_type = "apartment"
                        yield FormRequest(
                        "https://cdn.eazlee.com/eazlee/api/query_functions.php",
                        formdata=data,
                        headers=self.headers,
                        callback=self.populate_item,
                        meta={"external_id": external_id, "prop_type": property_type, "external_link":follow_url}
                    )
                    elif "Woonhuis" in prop_type:
                        property_type = "house"
                        yield FormRequest(
                        "https://cdn.eazlee.com/eazlee/api/query_functions.php",
                        formdata=data,
                        headers=self.headers,
                        callback=self.populate_item,
                        meta={"external_id": external_id, "prop_type": property_type, "external_link":follow_url}
                    )
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Number12_PySpider_" + self.country + "_" + self.locale)

        data = json.loads(response.body)

        prop_type = response.meta.get("prop_type")
        external_id = response.meta.get("external_id")
        
        item_loader.add_value("title", data[0]["street"])
        item_loader.add_value("external_link", response.meta.get("external_link"))

        item_loader.add_value("property_type", prop_type)

        bathroom_count = data[0]["bathrooms"]
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        if 'Energie label' in data[2].keys():
            energy_label = data[2]['Energie label']
            if energy_label:
                item_loader.add_value("energy_label", energy_label.strip())

        if 'Huisdieren:' in data[4].keys():
            pets_allowed = data[4]["Huisdieren:"]
            if 'ja' in pets_allowed.lower():
                item_loader.add_value("pets_allowed", True)
            elif 'nee' in pets_allowed.lower():
                item_loader.add_value("pets_allowed", False)

        desc = data[3]["description"]
        desc = re.sub('\s{2,}', ' ', desc)
        item_loader.add_value("description", desc )

        price = data[0]["set_price"]
        if price:
            if "Prijs op aanvraag" not in price:
                item_loader.add_value(
                    "rent", price.split(",")[0])
        item_loader.add_value("currency", "EUR")
        

        if "Borg:" in data[4]:
            item_loader.add_value(
                "deposit", data[4]["Borg:"].split("€")[1])

        item_loader.add_value(
            "external_id", external_id
        )

        square = data[2]["Oppervlakte"]
        if square:
            item_loader.add_value(
                "square_meters", square.split("m")[0]
            )
        room_count = data[0]["bedrooms"]
        if room_count!='0':
            item_loader.add_value("room_count", room_count)
        # if "Kamers" in data[2]:
        #     item_loader.add_value("room_count", data[2]["Kamers"])

        street = data[0]["street"]
        city = data[0]["city"]
        zipcode = data[0]["zipcode"]
        item_loader.add_value("address", street + " " + zipcode + " " + city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("city", city)
        
        if "Beschikbaar per" in data[2]:
            available_date = data[2]["Beschikbaar per"]
            if available_date:
                date_parsed = dateparser.parse(
                    available_date, date_formats=["%d-%m-%Y"]
                )
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
       
        
        if "Balkon:" in data[4].keys():
            item_loader.add_value("balcony", True)
        
        if "Interieur:" in data[4].keys():
            if "Gemeubileerd" in data[4]["Interieur:"]:
                item_loader.add_value("furnished", True)
        
        if "Dakterras:" in data[4].keys():
            if "Nee" in data[4]["Dakterras:"] or "No" in data[4]["Dakterras:"]:
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)

        images = [response.urljoin(item["middle"])
                    for item in data[1]
                ]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        item_loader.add_value("landlord_phone", "070 – 205 11 92")
        item_loader.add_value("landlord_name", "Number XII")
        item_loader.add_value("landlord_email", "home@number12.nl")

        
        item_loader.add_value("latitude", data[5]["lat"])
        item_loader.add_value("longitude", data[5]["lng"])
        yield item_loader.load_item()