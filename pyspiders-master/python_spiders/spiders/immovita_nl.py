# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
from python_spiders.loaders import ListingItem
import json
import dateparser
import re

class MySpider(Spider):
    name = 'immovita_nl'
    start_urls = ['https://immovita.nl/wp-content/themes/immovita/pararius/index.php?json&fields=lat,lng&lang=nl&bounds=&center=&zoom=&price=&rooms=&persons=&surface=&street=&district=']  # LEVEL 1
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'


    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)
        
        for item in data:
            item_id = item["id"]
            follow_url = f"https://immovita.nl/aanbod/woning/{item_id}/"
            lat = item["lat"]
            lng = item["lng"]
            
            yield Request(follow_url, callback=self.populate_item, meta={'lat': lat, 'lng': lng, 'item_id': item_id})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Immovita_PySpider_" + self.country + "_" + self.locale)
                
        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        latitude = str(response.meta.get("lat"))
        longitude = str(response.meta.get("lng"))

        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)


        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", str(response.meta.get("item_id")))

        
        description = response.xpath("//p[@class='description']/text()").get()
        if description:
            item_loader.add_value("description", description.replace("\n","").replace("\r","").replace("\t","").replace("\xa0","").strip())
            
        if "bathroom" in description.lower():
            bathroom_count = description.lower().split("bathroom")[0].strip().split(" ")[-1]
            if bathroom_count == "a":
                item_loader.add_value("bathroom_count", "1")
            elif "," in bathroom_count:
                bathroom_count = bathroom_count.split(",")[-1]
                item_loader.add_value("bathroom_count", bathroom_count)
            elif bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)
        
        if "Let op dubbele borg" in description:
            deposit = description.split("Let op dubbele borg")[1].split(".")[0].replace("\u20ac", "").strip()
            item_loader.add_value("deposit", deposit)
            
        item_loader.add_xpath("address", "//div/h1/text()")
        # item_loader.add_value("zipcode", zipcode)
        # item_loader.add_value("city", city)
                            
        property_type = (response.url).strip("/").split("https://immovita.nl")[1].split("/")[-2]
        if property_type and "woning" in property_type:
            item_loader.add_value("property_type", "house")
        elif property_type and "appartement" in property_type:
            item_loader.add_value("property_type", "apartment")
        else:
            return
        
        detail_content = response.xpath("//div[@class='base-1 base-lg-1-2']/p/text()").getall()
        if detail_content:
            for i in detail_content:
                if 'Oppervlakte' in i:
                    square_meters = i.strip().split(" ")[1]
                if 'Aantal slaapkamers' in i:
                    room_count = i.strip().split(" ")[2]
                if 'Beschikbaar per' in i:
                    available_date = i.strip().split(" ")[2]
                if 'Oplevering' in i:
                    try:
                        furnished = i.strip().split(" ")[1]
                        if furnished != 'Gestoffeerd':
                            furnished = True
                        else:
                            furnished = False
                    except:
                        furnished = False

        if not room_count:
            room_count = response.xpath("//p[@class='description']/text()[contains(.,'Kamer')]").get()
            if room_count:
                room_count = room_count.split("Kamer")[1].strip().split(" ")[0]
        if furnished:
            item_loader.add_value("furnished", furnished)
        
        item_loader.add_value("square_meters", square_meters)
        if "0" not in room_count:
            item_loader.add_value("room_count", room_count)
        
        if available_date and available_date.isalpha() != True:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        
        images = [x for x in response.xpath("//div[@class='swiper-wrapper']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        

        price = response.xpath("//span[contains(.,'€')]/text()").get()
        if price and price != "-": 
            price = price.split(",")[0].strip("€")
        
        item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")

        item_loader.add_value("landlord_phone", "070 820 98 42")
        item_loader.add_value("landlord_name", "Immovita")
        item_loader.add_value("landlord_email", "info@immovita.nl")
        yield item_loader.load_item()
