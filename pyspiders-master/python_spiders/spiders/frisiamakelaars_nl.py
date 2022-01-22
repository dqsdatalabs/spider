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
    name = 'frisiamakelaars_nl'
    start_urls = ['https://frisiamakelaars.nl/api/properties/available.json?nocache=1602238639843'] 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' 
    external_source='Frisiamakelaars_PySpider_netherlands_nl'# LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data["objects"]:
            if "rent" in item["buy_or_rent"]:
                prop_type = item.get('house_type')
                if prop_type == "Appartement":
                    prop_type = "apartment"
                elif prop_type == "Woonhuis":
                    prop_type = "house"
                else:
                    return

                follow_url = item["url"]
                lat = item["latitude"]
                lng = item["longitude"]
                zipcode = item["zip_code"]
                yield Request(follow_url, callback=self.populate_item, meta={"lat": lat, "lng": lng, "prop_type":prop_type, "zipcode" : zipcode})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Frisiamakelaars_PySpider_" + self.country + "_" + self.locale)

        external_id = response.url
        item_loader.add_value("external_id", external_id.split("-")[-1].split("#")[0].split("se")[1].strip())

        lat = response.meta.get("lat")
        lng = response.meta.get("lng")
        prop_type = response.meta.get("prop_type")
        item_loader.add_value("zipcode", response.meta.get("zipcode"))

        item_loader.add_value("external_link", response.url)
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        

        item_loader.add_value("property_type",prop_type)
        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        price = response.xpath("normalize-space(//tr[th[contains(.,'Huurprijs')]]/td/text())").extract_first()
        if price:
            item_loader.add_value("rent", price.split("€")[1].split("p")[0])
        item_loader.add_value("currency", "EUR")

        utilities = response.xpath("normalize-space(//tr[th[contains(.,'Servicekosten')]]/td/text())").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[1])

        room_count = response.xpath("//td[contains(., 'slaapkamers')]/text()").re_first(r'\d')
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("normalize-space(//tr[th[contains(.,'badkamer')]]/td//text())").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])

        square = response.xpath("normalize-space(//tr[th[contains(.,'Woonoppervlakte')]]/td/text())").get()
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])
            

        images = [response.urljoin(x)for x in response.xpath("//picture/img/@data-src").extract()]
        if images:
            img = list(set(images))
            item_loader.add_value("images", img)
        
        floor_images = [response.urljoin(x)for x in response.xpath("//button/figure//picture/img/@data-src").extract()]
        if floor_images:
            img = list(set(floor_images))
            item_loader.add_value("floor_plan_images", img)

        item_loader.add_xpath("floor","normalize-space(//tr[th[contains(.,'Aantal verdiepingen')]]/td/text())")

        try:
            available_date = "".join(response.xpath("normalize-space(//tr[th[contains(.,'Aanvaarding')]]/td/text())").extract()).strip()
            if "in overleg" not in available_date or "direct" not in available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        except:
            pass
        
        desc = "".join(response.xpath("//div[@class='panel__block tabs-description']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            

        label = "".join(response.xpath("//tr[th[contains(.,'Energielabel')]][not(contains(.,'Energielabel einddatum'))]/td/text()").extract())
        if label:
            item_loader.add_value("energy_label",label.strip())


        parking = "".join(response.xpath("//tr[th[contains(.,'Garage')]]/td//text()").extract())
        if parking:
            if "geen" in parking or "Geen" in parking: 
                if "parkeren" in desc.lower():
                    item_loader.add_value("parking",True)
                else:
                    item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        else:
            if "parkeerplaats" in desc or "parkeergarage" in desc:
                item_loader.add_value("parking", True)

        terrace = "".join(response.xpath("//tr[th[contains(.,'Voorzieningen')]]/td//text()[contains(.,'lift')]").extract()).strip()
        if terrace:
            item_loader.add_value("elevator", True)
        else:
            if "lift" in desc or "Lift" in desc:
                item_loader.add_value("elevator", True)
        
        if "balkon" in desc or "Balkon" in desc:
            item_loader.add_value("balcony", True)
        
        if "vaatwasser" in desc or "Vaatwasser" in desc:
            item_loader.add_value("dishwasher", True)
        
        if "wasmachine" in desc or "Wasmachine" in desc:
            item_loader.add_value("washing_machine", True)

        if "huisdier" in desc or "Huisdier" in desc:
            item_loader.add_value("pets_allowed", True)
        
        if "zwembad" in desc or "Zwembad" in desc:
            item_loader.add_value("swimming_pool", True)
        
        if "gemeubileerd" in desc or "Gemeubileerd" in desc:
            item_loader.add_value("furnished", True)

        if desc:
            if "niet gemeubileerd" in desc.replace(";","").lower():
                item_loader.add_value("furnished",False)
            elif "gestoffeerd" in desc.lower() or "gemeubileerd" in desc.lower() :
                item_loader.add_value("furnished",True)
            
            
        item_loader.add_xpath("address","normalize-space(//tr[th[contains(.,'Adres')]]/td/text())")
        item_loader.add_xpath("city","normalize-space(//tr[th[contains(.,'Plaats')]]/td/text())")

        item_loader.add_xpath("landlord_phone", "//ul[@class='contact-us-list']/li[1]/a/span/text()")
        item_loader.add_xpath("landlord_email", "//ul[@class='contact-us-list']/li[2]/a/span/text()")
        item_loader.add_value("landlord_name", "Frisia Makelaars B.V.")
        


        yield item_loader.load_item()