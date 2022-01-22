# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from python_spiders.loaders import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser


class MySpider(Spider):
    name = "expatrealestate_nl" # same with bnsrentalservice_nl
    start_urls = [
        "https://www.expat-realestate.nl/nl/realtime-listings/consumer"
    ] 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        
        jresp = json.loads(response.body)
        for item in jresp:
            item_loader = ListingLoader(response=response)
            if item["isRentals"] and item["rooms"] != 0 and type(item.get('type')) != bool:
                follow_url = response.urljoin(item.get('url'))

                property_type = item.get('mainType')
                if property_type == "apartment":
                    prop_type = "apartment"
                elif property_type == "house":
                    prop_type = "house"

                else:
                    return
                
                item_loader.add_value("external_link", follow_url)
                item_loader.add_value("address", item.get('address'))
                
                item_loader.add_value("zipcode", item.get('zipcode'))
                item_loader.add_value("city", item.get('city'))
                item_loader.add_value("rent",str(item.get('rentalsPrice')))
                item_loader.add_value("currency", "EUR")
                item_loader.add_value("square_meters",str(item.get('livingSurface')))
                item_loader.add_value("room_count",str(item.get('rooms')))
                item_loader.add_value("latitude",str(item.get('lat')))
                item_loader.add_value("longitude",str(item.get('lng')))

                furnished = item.get('isFurnished')
                if furnished == True:
                    item_loader.add_value("furnished", True)
                else:
                    item_loader.add_value("furnished", False)

                furnished = item.get('balcony')
                if furnished == True:
                    item_loader.add_value("balcony", True)
                else:
                    item_loader.add_value("balcony", False)
                
                yield response.follow(follow_url, self.populate_item, meta={'item': item_loader, 'property_type': prop_type})

    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        item_loader = response.meta.get("item")

        item_loader.add_value("external_source", "Expatrealestate_PySpider_" + self.country + "_" + self.locale)

        title="".join(response.xpath("//title/text()").getall())
        if title:
            item_loader.add_value("title", title.split("-")[0].strip())
            
        desc = "".join(response.xpath("//div[@class='object-description']/text()").extract()).replace("\n"," ")
        if desc:
            item_loader.add_value("description", desc)
        
        if "woning" in desc.lower():
            item_loader.add_value("property_type", "house")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        if "Kosten zijn circa" in desc:
            utilities = desc.split("Kosten zijn circa")[1].split("per")[0].replace("€","").strip()
            item_loader.add_value("utilities", utilities.split(",")[0])

        if "zwembad" in desc.lower():
            item_loader.add_value("swimming_pool", True)
            
        floor1 = response.xpath("//dl[@class='full-details']/dt[contains(.,'Verdiepingen')]/following-sibling::dd/text()").get()
        if " etage" in desc.lower():
            floor=desc.split(" etage")[0].strip().split(" ")[-1]
            floor=floor_trans(floor)
            if floor:
                item_loader.add_value("floor",floor.strip())
        elif floor1:
            item_loader.add_value("floor", floor1.strip())
            
        energy_label = response.xpath("//dl[@class='full-details']/dt[contains(.,'Energie')]/following-sibling::dd/text()").get()
        if "energielabel" in desc.lower():
            energy_label=desc.lower().split("energielabel")[1].replace("is","").replace(".","").replace(":","").strip().split(" ")[0]
            if "HET" not in energy_label.upper():
                item_loader.add_value(
                    "energy_label", energy_label.upper().replace(",","").replace(";","").replace("+","").replace("!",""))
        
        elif energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        bathroom=response.xpath("//dl[@class='full-details']/dt[contains(.,'badkamer')]/following-sibling::dd/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
            
        images = [response.urljoin(x)for x in response.xpath("//div[@class='photo-slider-slides']/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        available_date = response.xpath("//dl[@class='full-details']/dt[contains(.,'Oplevering')]/following-sibling::dd/text()").get()
        if available_date:
            date_parsed = dateparser.parse(
                        available_date, date_formats=["%d/%m/%Y"]
                    )
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        

        item_loader.add_value("landlord_phone", "070 - 212 25 89")
        item_loader.add_value("landlord_email", "info@expat-realestate.nl")
        item_loader.add_value("landlord_name", "Expat & Real Estate B.V")

        status=response.xpath("//dl[@class='full-details']/dt[contains(.,'Status')]/following-sibling::dd/text()").get()
        if "verhuurd" not in status.lower():
            yield item_loader.load_item()

def floor_trans(floor):
    
    if floor.replace("st","").replace("nd","").isdigit():
        if "2019" in floor.lower():
            return False
        else:
            return floor.replace("e","")
    elif "eerste" in floor.lower():
        return "1"
    elif "tweede" in floor.lower():
        return "2"
    elif "derde" in floor.lower():
        return "3"
    elif "vierde" in floor.lower():
        return "4"
    elif "vijfde" in floor.lower():
        return "5"
    elif "achtste" in floor.lower():
        return "8"
    elif "bovenste" in floor.lower() or "hoogste" in floor.lower():
        return "upper"
    else :
        return False