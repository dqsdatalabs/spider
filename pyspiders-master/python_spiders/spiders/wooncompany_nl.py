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
    name = 'wooncompany_nl'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.wooncompany.nl/nl/realtime-listings/consumer"}            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                        )

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)

        for item in data:
            if item["isRentals"] and not item["isSales"] and ("rented" not in item["statusOrig"]) and ("other" not in item["mainType"]):
                if "house" in item["mainType"] or "apartment" in item["mainType"]:
                    item_loader = ListingLoader(response=response)
                    item_loader.add_value("city", item["city"])
                    rent = item["price"]
                    if rent:
                        price = rent.split("p")[0].replace(" ","").replace(",","").replace("€","")
                        item_loader.add_value("rent", price)
                    item_loader.add_value("currency", "EUR")
                    furnished = item["isFurnished"]
                    if furnished:
                        if furnished == True:
                            item_loader.add_value("furnished",True )
                        elif furnished ==False:
                            item_loader.add_value("furnished",False)
                    
                    if item["balcony"] == True or item["balcony"] == "Ja" :
                        item_loader.add_value("balcony",True)
                    elif item["balcony"] == False:
                        item_loader.add_value("balcony",False)

                    item_loader.add_value("zipcode", item["zipcode"])
                    item_loader.add_value("address", item["address"])
                    item_loader.add_value("latitude", str(item["lat"]))
                    item_loader.add_value("longitude", str(item["lng"]))
                    
                    follow_url = response.urljoin(item["url"])
                    yield Request(follow_url, callback=self.populate_item, meta={"item":item_loader, "property_type": item["mainType"]})
                
    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = response.meta.get("item")
        item_loader.add_value("external_source","Wooncompany_PySpider_netherlands")
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//title/text()").get()
        item_loader.add_value("title", title)

        property_type = response.xpath("//dt[contains(.,'Woningtype')]/following-sibling::dd[1]/text()").get()
        if property_type:
            if "kamer" in property_type.lower(): item_loader.add_value("property_type", "room")
        
        if not item_loader.get_collected_values("property_type"): item_loader.add_value("property_type", response.meta["property_type"])

        available_date=response.xpath("//dl/dt[contains(.,'Oplevering')]/following-sibling::dd/text()[.!='Per direct' and .!='In overleg']").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        bathroom_count = response.xpath("//dl/dt[contains(.,'Badkamers')]/following-sibling::dd[1]/text()").get()  
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        room = ""
        room_count = "".join(response.xpath("//dl[@class='details-simple']//dt[contains(.,'Slaapkamers')][1]/following-sibling::dd[1]/text()").getall())  
       
        if room_count:
            room = room_count.strip()
        else:
            room_count = "".join(response.xpath("//dl[@class='details-simple']//dt[contains(.,'Kamers')][1]/following-sibling::dd[1]/text()").getall())
            if room_count:
                room = room_count.strip()

        item_loader.add_value("room_count", room.strip())

        meters = response.xpath("//dl/dt[contains(.,'Oppervlakte')]/following-sibling::dd[1]/text()").get()  
        if meters:
            item_loader.add_value("square_meters", meters.split("m²")[0].strip())

        description = " ".join(response.xpath("//li[@id='tab-omschrijving']/p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())

        images = [ response.urljoin(x) for x in response.xpath("//div[@class='responsive-slider-slides']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        floor_plan_images = [ response.urljoin(x) for x in response.xpath("//li[@id='tab-plattegronden']/div//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        item_loader.add_value("landlord_phone", "070-3468776")
        item_loader.add_value("landlord_email", "info@wooncompany.nl")
        item_loader.add_value("landlord_name", "WOONCOMPANY")

        yield item_loader.load_item()