# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser

class MySpider(Spider):
    name = 'helliwellandcompany_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.helliwellandcompany.co.uk/search/?instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=Apartment", "property_type": "apartment"},
            {"url": "https://www.helliwellandcompany.co.uk/search/?instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=Apartment+-+Duplex", "property_type": "apartment"},
            {"url": "https://www.helliwellandcompany.co.uk/search/?instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=Apartment+-+Mansion+Block", "property_type": "apartment"},
            {"url": "https://www.helliwellandcompany.co.uk/search/?instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=Flat", "property_type": "apartment"},
	        {"url": "https://www.helliwellandcompany.co.uk/search/?instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=Cottage+-+Detached", "property_type": "house"},
            {"url": "https://www.helliwellandcompany.co.uk/search/?instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=Flat+-+Ground+Floor", "property_type": "apartment"},
            {"url": "https://www.helliwellandcompany.co.uk/search/?instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=Flat+-+Studio", "property_type": "studio"},
            {"url": "https://www.helliwellandcompany.co.uk/search/?instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=Studio", "property_type": "studio"},
            {"url": "https://www.helliwellandcompany.co.uk/search/?instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=House+-+Detached", "property_type": "house"},
            {"url": "https://www.helliwellandcompany.co.uk/search/?instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=House+-+Semi-Detached", "property_type": "house"},
            {"url": "https://www.helliwellandcompany.co.uk/search/?instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=House+-+Terraced", "property_type": "house"},
            {"url": "https://www.helliwellandcompany.co.uk/search/?instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=House+-+Townhouse", "property_type": "house"},
            {"url": "https://www.helliwellandcompany.co.uk/search/?instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=Maisonette", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@id='search-results']/div/div[contains(@class,'property')]//h3/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
            seen = True
        
        if page == 1 or seen:
            base_url = response.meta.get("base_url", response.url.replace("/search/", "/search/page_count"))
            url = base_url.replace("/page_count",f"/{page}.html")
            yield Request(url, callback=self.parse, meta={"page": page+1, "base_url":base_url,"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "//h1/span/text()")
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Helliwellandcompany_Co_PySpider_"+ self.country)
        address = response.xpath("//h1/span/text()").extract_first()     
        if address:   
            item_loader.add_value("address",address.strip())
            zipcode = address.split(",")[-1].strip() 
            city = ""
            if "london" in zipcode.lower():
                zipcode = ""
                city = address.split(",")[-1].strip()
            city = address.split(",")[1]
            if city:
                item_loader.add_value("city",city.strip())
            if zipcode:
                item_loader.add_value("zipcode",zipcode.strip())

        available_date = response.xpath("//div[@class='key-features']//li[contains(.,'Available')]//text()[not(contains(.,'Now'))]").get()
        if available_date:
            try:
                available_date = available_date.split("Available")[1].strip()
                date_parsed = dateparser.parse(available_date, languages=['en'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m")
                    item_loader.add_value("available_date", date2)
            except:
                pass
            
        desc = " ".join(response.xpath("//span[@itemprop='description']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

            if not item_loader.get_collected_values("available_date"):
                pass


        room_count = response.xpath("//div[@class='property-room-numbers']//img[@alt='bedrooms']/following-sibling::text()[1]").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
   
        bathroom_count=response.xpath("//div[@class='property-room-numbers']//img[@alt='bathrooms']/following-sibling::text()[1]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        square_meters = response.xpath("//div[@class='key-features']//li[contains(.,'Sq') or contains(.,'sq')]//text()[not(contains(.,'Garden'))]").get()
        if square_meters:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sq.ft.|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",square_meters.replace(",",""))
            if unit_pattern:
                square_title=unit_pattern[0][0]
                sqm = str(int(float(square_title) * 0.09290304))
                item_loader.add_value("square_meters", sqm)
        elif not square_meters and desc:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|SQ FT|sq.ft.|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",desc.replace(",",""))
            if unit_pattern:
                square_title=unit_pattern[0][0]
                sqm = str(int(float(square_title) * 0.09290304))
                item_loader.add_value("square_meters", sqm)

        rent =" ".join(response.xpath("//span[@itemprop='price']//text()").extract())
        if rent:
            item_loader.add_value("rent_string", rent.replace(",","."))    

        # furnished = response.xpath("//div[@class='key-features']//li[contains(.,'furnished') or contains(.,'Furnished')]//text()").get()
        # if furnished:
        #     if "unfurnished" in furnished.lower() and "Furnished" in furnished:
        #         pass
        #     elif "unfurnished" in furnished.lower():
        #         item_loader.add_value("furnished", False)
        #     elif "furnished" in furnished.lower():
        #         item_loader.add_value("furnished", True)
        
        
        features = " ".join(response.xpath("//div[@class='key-features']/ul/li/text()").getall())
        if features:
            if "furnished or unfurnished" in features.lower():
                item_loader.add_value("furnished", True)
            elif "unfurnished" in features.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in features.lower():
                item_loader.add_value("furnished", True)


        parking = response.xpath("//div[@class='key-features']//li[contains(.,'parking') or contains(.,'Parking') or contains(.,'Garage') ]//text()").get()
        if parking:
            item_loader.add_value("parking", True)    
        balcony = response.xpath("//div[@class='key-features']//li[contains(.,'Balcony') or contains(.,'balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True) 

        floor = response.xpath("//div[@class='key-features']//li[contains(.,'Floor') or contains(.,'floor')]//text()[not(contains(.,'Wooden'))]").get()
        if floor:
            item_loader.add_value("floor", floor.lower().split("floor")[0].replace("fantastic","").strip())
       
        images = [response.urljoin(x) for x in response.xpath("//div[@id='property-thumbnails']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)   

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@class='modal-content'][./div/h4[contains(.,'Floorplan')]]/div[@class='modal-body']/img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)  

        map_coordinate = response.xpath("//script/text()[contains(.,'renderStreetview(')]").extract_first()
        if map_coordinate:
            map_coordinate = map_coordinate.split('.renderStreetview(')[1].split(');')[0]
            latitude = map_coordinate.split(',')[1].strip()
            longitude = map_coordinate.split(',')[2].strip()
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_phone", "020 8799 3810")
        item_loader.add_value("landlord_email", "info@helliwellandcompany.co.uk")
        item_loader.add_value("landlord_name", "Helliwell And Company")

        yield item_loader.load_item()