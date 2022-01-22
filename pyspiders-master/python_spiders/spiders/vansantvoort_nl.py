# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
from word2number import w2n
import dateparser


class MySpider(Spider):
    name = 'vansantvoort_nl'
    start_urls = ['https://www.vansantvoort.nl/wp-content/themes/vansantvoort-2020/json/get.php?getJSON=wonen-objects-json&lang=nl&cat=huurwoningen']  # LEVEL 1
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    
    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        
        for item in data["objects"]:
            url = item["permalink"]
            follow_url = f"https://www.vansantvoort.nl/wp-content/themes/vansantvoort-2020/json/get.php?getJSON=wonen-object-json-detail&lang=nl&ID={item['ID']}"
            yield Request(follow_url, callback=self.populate_item,meta={'url': url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        data = json.loads(response.body)
        
        if  type(data) == dict:
            
            item_loader.add_value("external_source", "Vansantvoort_PySpider_" + self.country + "_" + self.locale)
             
            if "value" in data["status"].keys():
                if data["status"]["value"] == "VERHUURD":
                    return

            item_loader.add_value("title", data["title"])
            # item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_link", response.meta.get("url"))

            if "Woonhuis type" in data["kenmerken"].keys():
                if "APPARTEMENT" in data["kenmerken"]["Woonhuis type"] or "TUSSENWONING" in data["kenmerken"]["Woonhuis type"] or "PORTIEKFLAT" in data["kenmerken"]["Woonhuis type"] or "BOVENWONING" in data["kenmerken"]["Woonhuis type"] or "BENEDENWONING" in data["kenmerken"]["Woonhuis type"] or "GALERIJFLAT" in data["kenmerken"]["Woonhuis type"]:
                    item_loader.add_value("property_type", "apartment")
                elif "VRIJSTAANDE_WONING" in data["kenmerken"]["Woonhuis type"] or "HALFVRIJSTAANDE_WONING" in data["kenmerken"]["Woonhuis type"] or "EENGEZINSWONING" in data["kenmerken"]["Woonhuis type"] or "PENTHOUSE" in data["kenmerken"]["Woonhuis type"] or "VILLA" in data["kenmerken"]["Woonhuis type"] or "BUNGALOW" in data["kenmerken"]["Woonhuis type"]:
                    item_loader.add_value("property_type", "house")
            elif "Soort woning" in data["kenmerken"].keys():
                if data["kenmerken"]["Soort woning"]["value"] != "":
                    if "APPARTEMENT" in data["kenmerken"]["Soort woning"]["value"] or "TUSSENWONING" in data["kenmerken"]["Soort woning"]["value"] or "PORTIEKFLAT" in data["kenmerken"]["Soort woning"]["value"] or "BOVENWONING" in data["kenmerken"]["Soort woning"]["value"] or "BENEDENWONING" in data["kenmerken"]["Soort woning"]["value"] or "GALERIJFLAT" in data["kenmerken"]["Soort woning"]["value"]:
                        item_loader.add_value("property_type", "apartment")
                    elif "VRIJSTAANDE_WONING" in data["kenmerken"]["Soort woning"]["value"] or "HALFVRIJSTAANDE_WONING" in data["kenmerken"]["Soort woning"]["value"] or "EENGEZINSWONING" in data["kenmerken"]["Soort woning"]["value"] or "PENTHOUSE" in data["kenmerken"]["Soort woning"]["value"] or "VILLA" in data["kenmerken"]["Soort woning"]["value"] or "BUNGALOW" in data["kenmerken"]["Soort woning"]["value"]:
                        item_loader.add_value("property_type", "house")
                elif data["kenmerken"]["Type woning"]["value"] != "":
                    if "APPARTEMENT" in data["kenmerken"]["Type woning"]["value"] or "TUSSENWONING" in data["kenmerken"]["Type woning"]["value"] or "PORTIEKFLAT" in data["kenmerken"]["Type woning"]["value"] or "BOVENWONING" in data["kenmerken"]["Type woning"]["value"] or "BENEDENWONING" in data["kenmerken"]["Type woning"]["value"] or "GALERIJFLAT" in data["kenmerken"]["Type woning"]["value"]:
                        item_loader.add_value("property_type", "apartment")
                    elif "VRIJSTAANDE_WONING" in data["kenmerken"]["Type woning"]["value"] or "HALFVRIJSTAANDE_WONING" in data["kenmerken"]["Type woning"]["value"] or "EENGEZINSWONING" in data["kenmerken"]["Type woning"]["value"] or "PENTHOUSE" in data["kenmerken"]["Type woning"]["value"] or "VILLA" in data["kenmerken"]["Type woning"]["value"] or "BUNGALOW" in data["kenmerken"]["Type woning"]["value"]:
                        item_loader.add_value("property_type", "house")
            item_loader.add_value("rent", str(data["financieel"]["huurprijs"]))
            item_loader.add_value("currency", "EUR")
            desc =  data["content"]
            if desc:
                desc = re.sub('\s{2,}', ' ', desc)
                item_loader.add_value("description",desc)
                if "badkamer" in desc.lower():
                    item_loader.add_value("bathroom_count","1")
                if "washing machine" in desc:
                    item_loader.add_value("washing_machine",True)
                if "Service costs: €" in desc:
                    try:
                        utilities = desc.split("Service costs: €")[1].split(",")[0].strip()
                        item_loader.add_value("utilities",utilities)
                    except:
                        pass
                if "Deposit:" in desc:
                    try:
                        deposit_value = desc.split("Deposit:")[1].split("month")[0].strip()
                        
                        deposit =""
                        if deposit_value.isdigit():
                            deposit = int(str(data["financieel"]["huurprijs"]))*int(deposit_value)
                        else:                            
                            deposit_number = w2n.word_to_num(deposit_value)
                            if deposit_number:
                                deposit = int(str(data["financieel"]["huurprijs"]))*int(deposit_number)
                        if deposit:
                            item_loader.add_value("deposit",deposit)
                    except:
                        pass
                if "Acceptance" in desc:
                    try:
                        available_date = desc.split("Acceptance")[1].strip().split(" ")[0].strip()
                        date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)
                    except:
                        pass
                if "Stookkosten (blokverwarming): €" in desc:
                    try:
                        heating_cost = desc.split("Stookkosten (blokverwarming): €")[1].split(",")[0].strip()
                        item_loader.add_value("heating_cost",heating_cost)
                    except:
                        pass
                if "parking" in desc:
                    item_loader.add_value("parking", True)


            

            item_loader.add_value("external_id", str(data["ID"]))

            if "value" in data["kenmerken"]["Woonoppervlakte"].keys():
                item_loader.add_value("square_meters", str(data["kenmerken"]["Woonoppervlakte"]["value"]))
            else:
                item_loader.add_value("square_meters", str(data["kenmerken"]["Woonoppervlakte"]))
            
            if "value" in data["kenmerken"]["Aantal kamers"].keys():
                item_loader.add_value("room_count", str(data["kenmerken"]["Aantal kamers"]["value"]))
            else:
                item_loader.add_value("room_count", str(data["kenmerken"]["Aantal kamers"]))


            if "Parkeerfaciliteiten" in data["kenmerken"].keys():
                item_loader.add_value("parking", True)
            

            if "Energielabel" in data["kenmerken"].keys():
                if data["kenmerken"]["Energielabel"]["value"]:
                    item_loader.add_value("energy_label", str(data["kenmerken"]["Energielabel"]["value"]))

            if "straatnaam" in data:
                item_loader.add_value("address", data["straatnaam"] + " " + data["postcode"] + " " + data["plaats"])
            else:
                item_loader.add_value("address",  data["postcode"] + " " + data["plaats"])
            item_loader.add_value("zipcode", data["postcode"])
            item_loader.add_value("city", data["plaats"])

            if "media" in data:
                images = [response.urljoin(data["media"]["images"][i]["media_image"]["url"])
                    for i in range(0,len(data["media"]["images"]))
                ]
                item_loader.add_value("images", images)
            
            parking = response.xpath("//ul[@class='features__list features__list--particularities']/li[.//div[contains(.,'Garagesoort')]]//span/text()").get()
            if parking:
                item_loader.add_value("parking", True)
            else:
                parking = response.xpath("//ul[@class='features__list features__list--particularities']/li[.//div[contains(.,'Parking facilities')]]//span/text()").get()
                if parking and "no" not in parking.lower():
                    item_loader.add_value("parking", True)


            item_loader.add_value("landlord_phone", "0031497513393")
            item_loader.add_value("landlord_name", "Van Santvoort Makelaars")
            item_loader.add_value("landlord_email", "info@eersel.vansantvoort.nl")
            
            item_loader.add_value("latitude", str(data["coordinates"]["lat"]))
            item_loader.add_value("longitude", str(data["coordinates"]["lng"]))
            if item_loader.get_collected_values("property_type"):
                yield item_loader.load_item()