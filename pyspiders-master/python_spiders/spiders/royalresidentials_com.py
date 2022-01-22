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

class MySpider(Spider):
    name = 'royalresidentials_com'

    start_urls = [
        "https://royalresidentials.com/lettings/"
    ]
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    headers = {
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "origin": "https://royalresidentials.com"
    }

    def parse(self, response):
        security = response.xpath("//input[@id='securityHouzezHeaderMap']/@value").get()
        prop_types = [
            {"type": "apartment", "property_type": "apartment"},
            {"type": "flat", "property_type": "apartment"},
	        {"type": "house", "property_type": "house"},
            {"type": "maisonette", "property_type": "house"},
            {"type": "penthouse", "property_type": "house"},
            {"type": "studio-flat", "property_type": "studio"},
        ]
        for t in prop_types:
            data = {
                "action": "houzez_half_map_listings",
                "location": "all",
                "status": "lettings",
                "type": t.get("type"),
                "bedrooms": "",
                "bathrooms": "",
                "min_price": "",
                "max_price": "",
                "min_area": "",
                "max_area": "",
                "search_lat": "",
                "search_long": "",
                "use_radius": "on",
                "search_location": "",
                "search_radius": "10",
                "sort_half_map": "featured_top",
                "security": f"{str(security)}",
                "paged": "0",
                "post_per_page": "20",
        }
        
            url = "https://royalresidentials.com/wp-admin/admin-ajax.php"        
            yield FormRequest(
                url,
                formdata=data,
                headers=self.headers,
                dont_filter=True,
                callback=self.parse_listing,
                meta={"property_type":t.get("property_type"), "data": data},
            )
    
    # 1. FOLLOWING
    def parse_listing(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 2)
        data={}
        try:
            data = json.loads(response.body)
        except:
            url = "https://royalresidentials.com/wp-admin/admin-ajax.php"        
            yield FormRequest(
                url,
                formdata=response.meta.get("data"),
                headers=self.headers,
                dont_filter=True,
                callback=self.parse_listing,
                meta={"property_type": response.meta.get("property_type"), "data": response.meta.get("data")},
            )
        if "properties" in data:
            for item in data["properties"]:
                lat = item["lat"]
                lng = item["lng"]
                yield Request(item["url"], callback=self.populate_item, meta={"property_type":property_type, "lat":lat, "lng":lng})

            form_data = response.meta.get("data")
            form_data["paged"] = str(page)
            url = "https://royalresidentials.com/wp-admin/admin-ajax.php"        
            yield FormRequest(
                url,
                formdata=form_data,
                headers=self.headers,
                dont_filter=True,
                callback=self.parse_listing,
                meta={"property_type":property_type, "data": form_data, "page":page+1},
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type =  response.meta.get('property_type')
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title",title)       
            if "studio" in title.lower():
                property_type = "studio"
        item_loader.add_value("property_type",property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Royalresidentials_PySpider_"+ self.country)
        rented = response.xpath("//span[@class='label-wrap']//span[contains(.,'Let Agreed')]//text()").get()
        if rented:
            return
        lat = response.meta.get("lat")
        lng = response.meta.get("lng")
        item_loader.add_value("latitude",str(lat))
        item_loader.add_value("longitude",str(lng))

        address = response.xpath("//div[@class='header-left']//address//text()").extract_first()     
        if address:   
            item_loader.add_value("address",address.strip())
     
        city = response.xpath("//li[@class='detail-city']/text()").extract_first()     
        if city:   
            item_loader.add_value("city",city.strip())

        zipcode = response.xpath("//li[@class='detail-zip']/text()").extract_first()     
        if zipcode:   
            item_loader.add_value("zipcode",zipcode.strip())

        rent = response.xpath("//span[@class='item-price']//text()").extract_first()
        if rent:
            if "pw" in rent.lower():
                numbers = re.findall(r'\d+(?:\.\d+)?', rent.replace(",","."))
                if numbers:
                    rent = int(numbers[0].replace(".",""))*4
                    rent = "£"+str(rent)
            item_loader.add_value("rent_string", rent.replace(",","."))   

        room_count =" ".join(response.xpath("//ul[@class='detail-amenities-list']/li/div[contains(.,'Bedroom')]//text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.split("Bedroom")[0])
        elif "studio" in property_type:
            item_loader.add_value("room_count", "1")
        elif not room_count:
            if title and " bed" in title.lower():
                room = title.lower().split(" bed")[0].strip()
                if room.isdigit():
                    item_loader.add_value("room_count", room)
        bathroom_count =" ".join(response.xpath("//ul[@class='detail-amenities-list']/li/div[contains(.,'Bathroom')]//text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Bathroom")[0])
        elif not bathroom_count:       
            bathroom_count =" ".join(response.xpath("//ul[@class='list-three-col']/li[strong[.='Bathrooms:']]/text()").getall())
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.split("Bathroom")[0])
        desc = " ".join(response.xpath("//div[contains(@class,'property-description')]/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        feature = " ".join(response.xpath("//div[contains(@class,'property-description')]/p/span[@class='text_exposed_show']//text()").extract())
        if feature:
            if "unfurnished" in feature.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in feature.lower():
                item_loader.add_value("furnished", True)
            if "washing machine" in feature.lower():
                item_loader.add_value("washing_machine", True)
            if "dishwasher " in feature.lower() or "dish washer " in feature.lower():
                item_loader.add_value("dishwasher", True)
            if "no pets" in feature.lower():
                item_loader.add_value("pets_allowed", False)
            if "lift " in feature.lower():
                item_loader.add_value("elevator", True)
            
            
        balcony = response.xpath("//div[@class='detail-features-left']//li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        images = [x for x in response.xpath("//div[@class='gallery-inner']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "020 8032 9342")
        item_loader.add_value("landlord_email", "info@royalresidentials.com")
        item_loader.add_value("landlord_name", "Royal Residentials")
        yield item_loader.load_item()