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
    name = 'jordanandhalstead_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Jordanandhalstead_Co_PySpider_united_kingdom'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.jordanandhalstead.co.uk/property-search/?address_keyword=&radius=&property_type=22&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&lat=&lng=&department=residential-lettings",
                    "https://www.jordanandhalstead.co.uk/property-search/?address_keyword=&radius=&property_type=134&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&lat=&lng=&department=residential-lettings",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.jordanandhalstead.co.uk/property-search/?address_keyword=&radius=&property_type=18&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&lat=&lng=&department=residential-lettings",
                    "https://www.jordanandhalstead.co.uk/property-search/?address_keyword=&radius=&property_type=124&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&lat=&lng=&department=residential-lettings",
                    "https://www.jordanandhalstead.co.uk/property-search/?address_keyword=&radius=&property_type=9&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&lat=&lng=&department=residential-lettings",
                    
                    ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://www.jordanandhalstead.co.uk/property-search/?address_keyword=&radius=&property_type=133&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&maximum_bedrooms=&lat=&lng=&department=residential-lettings",
                ],
                "property_type": "room"
            },
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
                
        for item in response.xpath("//div[contains(@class,'property-content')]"):
            follow_url = response.urljoin(item.xpath(".//div[contains(@class,'property-image')]/a/@href").get())
            status = item.xpath(".//span[contains(@class,'label')]/text()").get()
            if "to let" in status.lower():
                yield Request(
                    follow_url,
                    callback=self.populate_item,
                    meta={"property_type": response.meta.get('property_type')}
                )
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type": response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Jordanandhalstead_Co_PySpider_united_kingdom")
        
        prop_type = response.xpath("//h2[contains(.,'Studio')]").get()
        if prop_type:
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
                
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-1].strip())
        # zipcode=response.xpath("//title/text()").get()
        # if zipcode:
        #     item_loader.add_value("zipcode",zipcode.split("-")[-1].split("|")[0].strip())
        externalid=response.xpath("//link[@rel='shortlink']/@href").get()
        if externalid:
            item_loader.add_value("external_id",externalid.split("p=")[-1])
        
        rent = response.xpath("//h3/div[@class='price']/text()").get()
        item_loader.add_value("external_id", rent.strip())
        if rent:
            if "pw" in rent.lower():
                rent = rent.replace("\u00a3","").strip().split(" ")[0].replace("£","").replace(",","")
                item_loader.add_value("rent", int(float(rent))*4)
            else:
                rent = rent.replace("\u00a3","").strip().split(" ")[0].replace("£","").replace(",","")
                item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//div[@class='icon']//img[contains(@alt,'bedroom')]/parent::div/following-sibling::h6/text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        elif prop_type:
            item_loader.add_xpath("room_count", "1")
        
        bathroom_count = response.xpath("//div[@class='icon']//img[contains(@alt,'bathroom')]/parent::div/following-sibling::h6/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        description = " ".join(response.xpath("//div[@class='summary-contents']//p//text()").getall())
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))
        
        images = [x for x in response.xpath("//div[@class='slide']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
            
        item_loader.add_xpath("latitude", "//div/@data-lat")
        item_loader.add_xpath("longitude", "//div/@data-lng")
        
        item_loader.add_xpath("floor_plan_images", "//li[contains(.,'Floorplan')]/a/@href[not(contains(.,'#'))]")        
        
        item_loader.add_value("landlord_name", "Jordan & Halstead")
        phone = response.xpath("//a[contains(.,'Call')]/@href").get()
        if phone.split(":")[1].strip():
            item_loader.add_value("landlord_phone", phone.split(":")[1])
        else:
            item_loader.add_value("landlord_phone", "01244 794 093")
            
        item_loader.add_value("landlord_email", "info@jordanandhalstead.co.uk")
        
        yield item_loader.load_item()