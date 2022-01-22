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


class MySpider(Spider):
    name = 'grandrelocation_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source="Grandrelocation_PySpider_netherlands_nl"
    headers = {
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
        "origin": "https://www.grandrelocation.nl"
    }
    def start_requests(self):

        for i in range(0,60,6):
            data = {
                "action": "all_houses",
                "api": "25abab1e7685b3d249a473b6d8d371b5",
                "filter": "status=rent",
                "offsetRow": f"{i}",
                "numberRows": "6",
                "leased_wr_last": "false",
                "leased_last": "false",
                "sold_wr_last": "false",
                "sold_last": "false",
                "path": "/woning-aanbod",
                "html_lang": "nl",
           }
           
            url = "https://cdn.eazlee.com/eazlee/api/query_functions.php"
            yield FormRequest(
                url,
                formdata=data,
                headers=self.headers,
                callback=self.parse,
            )
     
    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)
        if data:
        
            for item in data:
                follow_url = "https://www.grandrelocation.nl/woning?Amsterdam/" + item["street"].replace(" ", "-") + "/" + item["house_id"]
                prop_type = item["house_type"]
                if prop_type == "Appartement":
                    prop_type = "apartment"
                elif prop_type == "Woonhuis":
                    prop_type = "house"
                    
                external_id = item["house_id"]

                data = {
                "action": "property",
                "url": follow_url,
                "api": "25abab1e7685b3d249a473b6d8d371b5",
                "path":"/woning",
                }
            
                yield FormRequest(
                    "https://cdn.eazlee.com/eazlee/api/query_functions.php",
                    formdata=data,
                    headers=self.headers,
                    callback=self.populate_item,
                    meta={"external_id": external_id, "prop_type": prop_type, "follow_url":follow_url}
                )
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)


        item_loader.add_value("external_source", self.external_source)
        
        data = json.loads(response.body)

        prop_type = response.meta.get("prop_type")
        external_id = response.meta.get("external_id")
        item_loader.add_value("external_link", response.meta.get("follow_url"))
        
        item_loader.add_value("title", data[0]["street"])
        item_loader.add_value("property_type", prop_type)
        item_loader.add_value("external_id", external_id)
        square_meters = data[0]["surface"]
        item_loader.add_value("square_meters",square_meters.replace("m<sup>2</sup>",""))
        if "Slaapkamers" in data[2].keys():
            item_loader.add_value("room_count", data[2]["Slaapkamers"])

        desc = data[3]["description"]
        item_loader.add_value("description", desc)
        item_loader.add_value("latitude", data[5]["lat"])
        item_loader.add_value("longitude", data[5]["lng"])
        item_loader.add_value("zipcode", data[0]["zipcode"])
        item_loader.add_value("city", data[0]["city"])
        item_loader.add_value("address", ("{} {},{} ".format(data[0]["street"],data[0]["zipcode"],data[0]["city"])))

        uti = ""
        if "Service costs" in desc:
            utilines = desc.split("Service costs")[1]
            if "euros" in utilines:
                uti = utilines.split("euros")[0].replace(":","")
            elif "Euros" in utilines:
                uti = utilines.split("Euros")[0].replace("per month are","")
            item_loader.add_value("utilities", uti)

        bathrooms = int(data[0]["bathrooms"])
        if bathrooms != 0:
            item_loader.add_value("bathroom_count", bathrooms)

        images = []
        image = data[1]
        for i in image:
            images.append(i.get("middle"))
        item_loader.add_value("images", list(set(images)))
        price = data[0]["set_price"]
        
        if price:
            item_loader.add_value("rent", price.split(",")[0])
            item_loader.add_value("currency", "EUR")

        try:
            if data[4]["Interieur:"] == "Kaal":
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
            
            balcony = data[4]["Balkon:"]
            if balcony:
                item_loader.add_value("balcony", True)

            label = data[2]["Energie label"]
            if label:
                item_loader.add_value("energy_label", label)

        except:
            pass
        try:
            if "Dakterras" in data[4]:
                terrace = data[4]["Dakterras:"]
                if terrace:
                    if terrace == "Nee":
                        item_loader.add_value("terrace", False)
                    elif "Ja" in terrace:
                        item_loader.add_value("terrace", True)
        except:
            pass

        try:

            available_date = data[0]["available_at"]
            if "direct" not in available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
                
        except:
            pass

        item_loader.add_value("landlord_phone", "+3120 620 0813")
        item_loader.add_value("landlord_email", "info@grandrelocation.nl")
        item_loader.add_value("landlord_name", "Grand Relocation")


        yield item_loader.load_item()