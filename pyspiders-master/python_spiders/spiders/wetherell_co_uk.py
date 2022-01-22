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
    name = 'wetherell_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.wetherell.co.uk/search.ljson?channel=lettings&fragment=page-1",
                ],
                "property_type": "apartment"
            },
	        # {
            #     "url": [
            #         "https://wetherell.co.uk/mayfair-properties-rent/houses/?exclude=1&pg=1"
            #     ],
            #     "property_type": "house"
            # }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        page = response.meta.get('page', 2)
        seen = False
        if data["properties"]:
            for item in data["properties"]:
                follow_url = response.urljoin(item["property_url"])
                yield Request(follow_url, callback=self.populate_item,meta={"item":item,"property_type": response.meta.get('property_type')})
                seen=True
            
            if page == 2 or seen:
                url = f"https://www.wetherell.co.uk/search.ljson?channel=lettings&fragment=page-{page}"
                yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        externallink=response.url
        if externallink:
            if not "properties" in externallink:
                return

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Wetherell_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("/")[-2])
        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title.strip())
            item_loader.add_value("address", title.strip())
            item_loader.add_value("zipcode", title.split(",")[-1].strip())
            item_loader.add_value("city", title.split(",")[-2].strip())
        
        room=response.xpath("//span[.='bedrooms']/parent::span/text()").get()
        if room:
            room=room.replace("\n","").strip()
            room=re.findall("\d+",room)
            item_loader.add_xpath("room_count",room)
        roomcheck=item_loader.get_output_value("room_count")
        if not roomcheck:
            room1=response.xpath("//span[.='bedroom']/parent::span/text()").get()
            if room1:
                room1=room1.replace("\n","").strip()
                room1=re.findall("\d+",room1)
                item_loader.add_xpath("room_count",room1)
 
        bathroom=response.xpath("//span[.='bathrooms']/parent::span/text()").get()
        if bathroom:
            bathroom=bathroom.replace("\n","").strip()
            bathroom=re.findall("\d+",bathroom)
            item_loader.add_xpath("bathroom_count",bathroom)
        bathcheck=item_loader.get_output_value("bathroom_count")
        if not bathcheck:
            bath=response.xpath("//span[.='bathroom']/parent::span/text()").get()
            if bath:
                bath=bath.replace("\n","").strip()
                bath=re.findall("\d+",bath)
                item_loader.add_xpath("bathroom_count",bath)

        
        rent = response.xpath("//span[@class='property-price']/data/text()").get()
        if rent:
            rent = rent.replace(",","").strip()
            rent=re.findall("\d+",rent)
            item_loader.add_value("rent", str(int(rent[0])*4)) 
            item_loader.add_value("currency", "GBP")
     
        description = " ".join(response.xpath("//div[@class='content page-content']//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        images = [x for x in response.xpath("//ul[@class='slides']//li//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [x for x in response.xpath("//div[contains(@class,'floorplan-area')]//img//@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        LatLng="".join(response.xpath("//script[contains(.,'LatLng')]/text()").getall())
        if LatLng:
            latlngindex=LatLng.find("LatLng")
            latlng=LatLng[latlngindex:]
            latlng=latlng.split(");")[0]
            lat=latlng.split(",")[0]
            lng=latlng.split(",")[-1]
            if lat:
                item_loader.add_value("latitude",lat.replace("LatLng(","").strip())
            if lng:
                item_loader.add_value("longitude",lng.replace("-","").replace(")","").strip())

        features =response.xpath("//ul[@class='property-features']//li//text()").getall()
        if features:
            for i in features:
                if "garage" in i.lower() or "parking" in i.lower():
                    item_loader.add_value("parking", True)  
                if "terrace" in i.lower():
                    item_loader.add_value("terrace", True) 
                if "balcony" in i.lower():
                    item_loader.add_value("balcony", True)
                if "furnished" in i.lower():
                    item_loader.add_value("furnished", True)

                
        item_loader.add_value("landlord_name", "Wetherell")
        item_loader.add_value("landlord_phone", "+44 (20) 7493 6935")
        item_loader.add_value("landlord_email", "info@wetherell.co.uk")

        status = response.xpath("//header//span[contains(@class,'status')]/text()").get()
        if status != 'Let':
            yield item_loader.load_item()