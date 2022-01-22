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
    name = 'viadaan_nl'
    headers = {
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
        "origin": "https://www.viadaan.nl"
    }
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'

    s= {}

    def start_requests(self):
        for i in range(0,150,10):
            data = {
                "action": "all_houses",
                "api": "2d8bbcb25ee78ee6c4ffb7de7a361f65",
                "filter": "status=rent",
                "offsetRow": f"{i}",
                "numberRows": "10",
                "leased_wr_last": "false",
                "leased_last": "false",
                "sold_wr_last": "false",
                "sold_last": "false",
                "path": "/huren-via-daan?status=rent",
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
                follow_url = "https://www.viadaan.nl/woning?Lelystad/" + item["street"].replace(" ", "-") + "/" + item["house_id"]
                prop_type = item["house_type"]
                #{'Appartement': 'OK', 'Woonhuis': 'OK', 'Parkeergelegenheid': 'OK'}
                if prop_type and "Appartement" in prop_type:
                    prop_type = "apartment"
                elif prop_type and "Woonhuis" in prop_type:
                    prop_type = "house"
                else:
                    prop_type = None
                external_id = item["house_id"]
                
                data = {
                "action": "property",
                "url": follow_url,
                "api": "2d8bbcb25ee78ee6c4ffb7de7a361f65",
                "path":"/woning",
            }
            
                yield FormRequest(
                    "https://cdn.eazlee.com/eazlee/api/query_functions.php",
                    formdata=data,
                    headers=self.headers,
                    callback=self.populate_item,
                    meta={"external_id": external_id, "prop_type": prop_type, "external_link":follow_url}
                )
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        data = json.loads(response.body)
        item_loader.add_value("external_source", "Viadaan_PySpider_" + self.country + "_" + self.locale)

        

        prop_type = response.meta.get("prop_type")
        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else:
            return
        rented = data[0]["front_status"]
        if "Verhuurd" in rented:
            return
        external_id = response.meta.get("external_id")
        item_loader.add_value("external_link", response.meta.get("external_link"))
        item_loader.add_value("title", data[0]["street"])
        item_loader.add_value("external_id", external_id)
        square_meters = data[0]["surface"]
        item_loader.add_value("square_meters",square_meters.replace("m<sup>2</sup>","") )
        room_bath= data[0]["bathrooms"]
        if room_bath != "0":
            item_loader.add_value("bathroom_count", room_bath)

        room_count = data[2]["Kamers"]
        if "studio" in item_loader.get_collected_values("property_type") and "0" in room_count:
            item_loader.add_value("room_count", "1")
        else:
            if "0" not in room_count:
                item_loader.add_value("room_count", room_count)
            
        
        desc = data[3]["description"]
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
            if " verdieping" in desc:
                try:
                    floor = desc.split(" verdieping")[0].strip().split(" ")[-1].strip()
                    if "naar" not in floor or "De" not in floor:
                        item_loader.add_value("floor", floor.replace("/>\n",""))
                except:
                    pass
        item_loader.add_value("latitude", data[5]["lat"])
        item_loader.add_value("longitude", data[5]["lng"])
        item_loader.add_value("zipcode", data[0]["zipcode"])
        item_loader.add_value("city", data[0]["city"])
        address_street=""
        try :
            address_street = data[0]["street"] 
        except:
            pass 
        if address_street:
            address= data[0]["street"] +", "+ data[0]["city"]  
            item_loader.add_value("address", address)
        elif not address_street:
            address =  data[0]["city"]
            item_loader.add_value("address", address)
        

        images = []
        image = data[1]
        for i in image:
            images.append(i.get("middle"))
        item_loader.add_value("images", images)

        price = data[0]["set_price"]
        if price:
            item_loader.add_value("rent", price.split(",")[0])
        item_loader.add_value("currency", "EUR")


        available_date = data[0]["available_at"]
        if available_date:
            if available_date.isalpha() != True:
                date_parsed = dateparser.parse(available_date, date_formats=["%d-%m-%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                elif dateparser.parse(available_date, date_formats=["%Y-%m-%d"]):
                    item_loader.add_value("available_date", available_date)
        
        try:
            energy = data[2]["Energie label"]
            if energy:
                item_loader.add_value("energy_label", energy)
        except:
            pass
        try:
            deposit = data[4]["Borg:"]
            if deposit:
                item_loader.add_value("deposit", deposit.split("€")[1])
        except:
            pass
        try:
            terrace = data[4]["Balkon:"]
            if terrace:
                item_loader.add_value("balcony", True)
        except:
            pass
        try:
            terrace = data[4]["Interieur:"]
            if terrace:
                if "gemeubileerd" in terrace.lower():
                    item_loader.add_value("furnished", True)
                else:
                    item_loader.add_value("furnished", False)
        except:
            pass
        try:
            terrace = data[4]["Dakterras:"]
            if "Ja" in terrace:
                item_loader.add_value("terrace", True)
            elif "Nee" in terrace:
                item_loader.add_value("terrace", False)
        except:
            pass

        item_loader.add_value("landlord_phone", "0031(0)43-7600076")
        item_loader.add_value("landlord_email", "info@viadaan.nl")
        item_loader.add_value("landlord_name", "ViaDaan")

        yield item_loader.load_item()