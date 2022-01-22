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
    name = 'kimmel_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1
    external_source = "Kimmel_PySpider_netherlands_nl"
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.kimmel.nl/nl/realtime-listings/consumer"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)
        for item in data:
            if item["isRentals"]:
                follow_url = response.urljoin(item["url"])
                lat = item["lat"]
                lng = item["lng"]
                price = item["rentalsPrice"]
                city = item["city"]
                zipcode = item["zipcode"]
                property_type  = item["mainType"]
                if "house" in property_type or "apartment" in property_type:
                    yield Request(follow_url, callback=self.populate_item, meta={"lat": lat, "lng": lng,"city": city, "zipcode": zipcode, "price": price, "property_type": property_type})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rented = response.xpath("//dt[contains(.,'Status')]/following-sibling::dd[1]/text()").get()
        if "Verhuurd" in rented:
            return
        item_loader.add_value("external_source", self.external_source)
        
        latitude = str(response.meta.get("lat"))
        longitude = str(response.meta.get("lng"))
        price = str(response.meta.get("price"))
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        
        item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")

        desc = "".join(response.xpath("//div[@class='expand-content']/p/text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc.strip())
            if "No pets allowed" in desc:
                item_loader.add_value("pets_allowed",False)
            
        if latitude and longitude:
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        address = ", ".join(response.xpath("//div[h1]//text()[normalize-space()]").extract())
        if address:
            address = re.sub('\s{2,}', ' ', address)
            item_loader.add_value("address",address.strip()) 
        # item_loader.add_xpath("address", "//div[h1]/p//text()")
        utilities = response.xpath("//dt[contains(.,'Servicekosten')]/following-sibling::dd[1]//text()[contains(.,'€')]").get()
        if utilities:    
            item_loader.add_value("utilities",utilities.split("€")[1].strip())
        item_loader.add_value("zipcode", str(response.meta.get("zipcode")))
        item_loader.add_value("city", str(response.meta.get("city")))

        item_loader.add_value("property_type", response.meta.get("property_type"))
        
        square_meters = response.xpath("//dt[contains(.,'Woonoppervlakte')]/following-sibling::dd[1]/text()").get()
        if square_meters:
            square_meters = square_meters.strip("m²")
            item_loader.add_value("square_meters", square_meters)
       
        room_count = response.xpath("//dt[contains(.,'Aantal slaapkamers')]/following-sibling::dd[1]/text()").get()
        if room_count:    
            item_loader.add_value("room_count", room_count)
        elif not room_count:
            room1=response.xpath("//i[@class='realtor realtor-floorplan']/following-sibling::text()").getall()
            room1=re.findall("\d+",room1)
            item_loader.add_value("room_count",room1)
            
        
        available_date = response.xpath("//dt[contains(.,'Oplevering')]/following-sibling::dd[1]/text()").get()
        if available_date and available_date.replace(" ","").isalpha() != True:
            try:
                available_date_list = available_date.split(" ")
                available_date = available_date_list[2] + " " + available_date_list[3] + " " + available_date_list[4]
            except:
                pass
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        

        images = [x for x in response.xpath("//div[@class='responsive-gallery-item']/div/img/@data-src").getall()]
        if images:
            item_loader.add_value("images", list(set(images)))
        
 
        floor_plan_images = [x for x in response.xpath("//div[@class='responsive-slider-slides']/div/img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)


        furnished = response.xpath("//dt[contains(.,'Inrichting')]/following-sibling::dd[1]/text()").get()
        if furnished:
            if "gestoffeerd" in furnished.lower() or "gemeubileerd" in furnished.lower():
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)


        elevator = response.xpath("//dt[contains(.,'Voorzieningen')]/following-sibling::dd[1]/text()").get()
        if elevator:
            if "Lift" in elevator:
                item_loader.add_value("elevator", True)
                   

        floor = response.xpath("//dt[contains(.,'verdiepingen')]/following-sibling::dd[1]/text()").get()
        if floor:
            item_loader.add_value("floor", floor)

    
        parking = response.xpath("//dt[contains(.,'garage')]/following-sibling::dd[1]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        balcony = response.xpath("//dt[contains(.,'Balkon')]/following-sibling::dd[1]/text()").get()
        if balcony:
            if "Ja" in balcony:
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)

        item_loader.add_value("landlord_phone", "+31 (0)70 3 262 726")
        item_loader.add_value("landlord_email", "info@kimmel.nl")
        item_loader.add_value("landlord_name", "Makelaarskantoor Kimmel & Co")

        yield item_loader.load_item()