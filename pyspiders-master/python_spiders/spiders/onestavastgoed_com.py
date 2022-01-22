# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re
import dateparser

class MySpider(Spider):
    name = 'onestavastgoed_com'
    start_urls = ['https://www.onesta-vastgoed.com/nl/realtime-listings/consumer'] 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)
        
        for item in data:
            if item["isRentals"]:
                follow_url = response.urljoin(item["url"])
                lat = item["lat"]
                lng = item["lng"]
                zipcode = item["zipcode"]
                city = item["city"]
                property_type = item["mainType"]
                if "apartment" in property_type or "house" in property_type:
                    yield Request(follow_url, callback=self.populate_item, meta={'lat': lat, 'lng': lng,'zipcode': zipcode, 'city': city, "property_type" : property_type})
                
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)     
        rented = response.xpath("//span[@class='details-status']/text()").extract_first()
        if rented == "Verhuurd":
            return
        item_loader.add_value("external_source", "Onestavastgoed_PySpider_" + self.country + "_" + self.locale)

        title = response.xpath("//div[@class='container']//h1/text()").extract_first()
        item_loader.add_value("title", title)
        item_loader.add_value('latitude', str(response.meta.get('lat')))
        item_loader.add_value('longitude', str(response.meta.get('lng')))
        item_loader.add_value('external_link', response.url)
        prop_type = response.meta.get("property_type")

        property_type = response.xpath("//dl[@class='details-simple']/dt[.='Woningtype']/following-sibling::dd[1]/text()").extract_first()
        if property_type:   
            if "woning" in property_type.lower():
                prop_type ="house"
                
        item_loader.add_value("property_type", prop_type)
        desc = "".join(response.xpath("//p[@class='object-description']//text()").extract())
        desc = re.sub('\s{2,}', ' ', desc)
        item_loader.add_value("description", desc.strip())

        item_loader.add_value("address", title)
        energy_label = response.xpath("//p[@class='object-description']//text()[contains(.,'Energielabel') or contains(.,'energielabel') ]").extract_first()
        if energy_label:
            try:
                energy = energy_label.strip().split(" ")[-1].strip()
                energy = energy.replace(";","").replace(".","")
                if energy.isalpha():
                    item_loader.add_value("energy_label", energy)               
            except:
                pass
        # city = "".join(response.xpath("//dl[@class='details-simple']/dt[.='Postcode']/following-sibling::dd[1]/text()").get())
        # if city:
        #     item_loader.add_value("city", city.split()[-2] + " " + city.split()[-1])
        #     item_loader.add_value("zipcode", city.split()[0])

        item_loader.add_value("city", str(response.meta.get('city')))
        item_loader.add_value("zipcode", str(response.meta.get('zipcode')))
        
        
        item_loader.add_xpath("square_meters", "//dl[@class='details-simple']/dt[.='Oppervlakte']/following-sibling::dd[1]/text()")
        
        item_loader.add_xpath("room_count", "//dl[@class='details-simple']/dt[.='Slaapkamers']/following-sibling::dd[1]/text()")
        item_loader.add_xpath("bathroom_count", "//dl[@class='details-simple']/dt[.='Badkamers']/following-sibling::dd[1]/text()")
        
        price = response.xpath("//dl[@class='details-simple']/dt[.='Prijs']/following-sibling::dd[1]/text()").extract_first()

        if price:        
            item_loader.add_value("rent", price.split(" ")[-3])
            item_loader.add_value("currency", "EUR")
        utilities = response.xpath("//dl[@class='details-simple']/dt[.='Servicekosten']/following-sibling::dd[1]/text()").extract_first()
        if utilities:        
            item_loader.add_value("utilities",utilities )
 
        images = [response.urljoin(x)for x in response.xpath("//div[@class='responsive-slider-slides']/div/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_value("external_id", response.url.split("/")[-1])
        
        available_date = response.xpath("//dl[@class='details-simple']/dt[.='Oplevering']/following-sibling::dd[1]/text()[.!='Per direct' and .!='In overleg']").get()
        if available_date:
            new_date = dateparser.parse(available_date).strftime("%Y-%m-%d")
            item_loader.add_value("available_date", new_date)

        furnished = response.xpath("//dl[@class='details-simple']/dt[.='Inrichting']/following-sibling::dd[1]/text()[contains(. ,'Gestoffeerd') or contains(. ,'furnished') or contains(. ,'Gemeubileerd')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        parking = response.xpath("//dl[@class='details-simple']/dt[.='Parkeren']/following-sibling::dd[1]/text()[contains(. ,'Garage') or contains(. ,'Parkeergarage') or contains(. ,'parkeren') ]").get()
        if parking:
            item_loader.add_value("parking", True) 

        dishwasher =  "".join(response.xpath("//p[@class='object-description']//text()").extract())
        if "vaatwasmachine" in dishwasher or "dishwasher" in dishwasher:
            item_loader.add_value("dishwasher", True)   

        balcony =  "".join(response.xpath("//p[@class='object-description']//text()").extract())
        if "balcony" in balcony:
            item_loader.add_value("balcony", True)      


        item_loader.add_value("landlord_phone", "070 - 345 95 22")
        item_loader.add_value("landlord_email", "info@onesta-vastgoed.com")
        item_loader.add_value("landlord_name", "onesta-vastgoed")

        yield item_loader.load_item()