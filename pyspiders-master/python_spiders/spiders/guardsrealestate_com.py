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
    name = 'guardsrealestate_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.guardsrealestate.com/property/search?list=L&postcode=&type=Apartment&beds=1",
                    "https://www.guardsrealestate.com/property/search?list=L&postcode=&type=Flat&beds=1"
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.guardsrealestate.com/property/search?list=L&postcode=&type=Double+Room&beds=1"
                ],
                "property_type": "house"
            }
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
        for item in response.xpath("//a[contains(@class,'list-item')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "GuardsRealestate_PySpider_united_kingdom")
        
        externalid=response.xpath("//div[@class='h-addr']/text()").get()
        if externalid:
            externalid=externalid.split(":")[-1].strip()
            item_loader.add_value("external_id", externalid)

        title ="".join(response.xpath("//title//text()").get())
        if title:
            title=title.replace("\n","").replace("\t","")
            title=re.sub('\s{2,}',' ',title.strip())
            item_loader.add_value("title", title)
        address =response.xpath("//h1[@class='prop-name']/text()").get()
        if address:
            item_loader.add_value("address", address)
            city=address.split(" ")[-1]
            item_loader.add_value("city", city.strip()) 
            # zipcode=address.split(",")[-1]
            # item_loader.add_value("zipcode", zipcode.strip())
        rent =response.xpath("//h1[@class='prop-name']/div/text()").get()
        if rent:
            rent = rent.replace(",","").strip()
            if "pw" in rent.lower():
                rent=re.findall("\d+",rent)              
                item_loader.add_value("rent", int(rent[0])*4)
            elif "pcm" in rent.lower():
                rent=re.findall("\d+",rent)
                item_loader.add_value("rent", int(rent[0]))
        item_loader.add_value("currency", "GBP") 
        room_count =response.xpath("//span[contains(.,'Bed') or contains(.,'bed')]/text()").get()
        if room_count:
            room_count =re.findall("\d",room_count)
            item_loader.add_value("room_count", room_count)
        bathroom_count =response.xpath("//span[contains(.,'Bath') or contains(.,'bath')]/text()").get()
        if bathroom_count:
            bathroom_count =re.findall("\d",bathroom_count)
            item_loader.add_value("bathroom_count", bathroom_count)
        squaremeters=response.xpath("//span[contains(.,'m2')]/text()").get()
        if squaremeters:
            squaremeters =squaremeters.split("m2")[0].strip()
            item_loader.add_value("square_meters", int(float(squaremeters)))
        desc = "".join(response.xpath("//div[@class='detail-grid h-details']/div/p/text()").get())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())          
            item_loader.add_value("description", desc)
        images = [x for x in response.xpath("//a[@class='carousel-item']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [x for x in response.xpath("//div[@class='modal-content']/img/@src").getall()]
        if floor_plan_images:  
            image=str("".join(floor_plan_images))       
            if "estate" in image.lower():
                item_loader.add_value("floor_plan_images", floor_plan_images)
        LatLng="".join(response.xpath("//script[contains(.,'lng')]/text()").getall())
        if LatLng:
            latlngindex=LatLng.find("lat")
            latlng=LatLng[latlngindex:]
            latlng=latlng.split("}")[0]
            lat=latlng.split("lat:")[-1].split(",")[0]
            if lat:
                item_loader.add_value("latitude",lat)
            lng=latlng.split("lng:-")[-1]
            if lng:
                item_loader.add_value("longitude",lng)
        features =response.xpath("//div[@class='row det-am']//div/span/text()").getall()
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
        item_loader.add_value("landlord_name", "Guards Real Estate")
        item_loader.add_value("landlord_phone", "+44 (0) 20 3633 1271")


          

        yield item_loader.load_item()