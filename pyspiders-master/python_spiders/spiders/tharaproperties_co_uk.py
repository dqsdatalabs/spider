# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'tharaproperties_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.tharaproperties.co.uk/residential-lettings?page=1&view=list&distance=6&propertyTypes[]=88",
                    "https://www.tharaproperties.co.uk/residential-lettings?page=1&view=list&distance=6&propertyTypes[]=92",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.tharaproperties.co.uk/residential-lettings?page=1&view=list&distance=6&propertyTypes[]=4",
                    "https://www.tharaproperties.co.uk/residential-lettings?page=1&view=list&distance=6&propertyTypes[]=5",
                    "https://www.tharaproperties.co.uk/residential-lettings?page=1&view=list&distance=6&propertyTypes[]=6",
                    "https://www.tharaproperties.co.uk/residential-lettings?page=1&view=list&distance=6&propertyTypes[]=7",
                    "https://www.tharaproperties.co.uk/residential-lettings?page=1&view=list&distance=6&propertyTypes[]=19",
                    "https://www.tharaproperties.co.uk/residential-lettings?page=1&view=list&distance=6&propertyTypes[]=22",
                    "https://www.tharaproperties.co.uk/residential-lettings?page=1&view=list&distance=6&propertyTypes[]=91",                  
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://www.tharaproperties.co.uk/residential-lettings?page=1&view=list&distance=6&propertyTypes[]=89",
                ],
                "property_type": "studio"
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
        
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//a[contains(@class,'card-link ')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = response.url.replace(f"page={page-1}", f"page={page}")
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rented = response.xpath("//div[@class='property-label']/text()[.='Let Agreed']").extract_first()
        if rented:return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Tharaproperties_Co_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//title/text()")

        room = response.xpath("substring-before(//div[@class='col-24 order-xl-5 text-center']/h1/text(),'Bedroom')").extract_first()
        if room:
            item_loader.add_value("room_count", room.strip())

        address = response.xpath("//div[@class='col-24 order-xl-5 text-center']/h2/text()").extract_first()
        if address:
            item_loader.add_value("address", address.strip())
            zipcode = ""
            if not address.strip().replace(",","").split(" ")[-2].isalpha():
                zipcode = zipcode+(address.strip().split(" ")[-2])
            if not address.strip().replace(",","").split(" ")[-1].isalpha():
                zipcode = zipcode+" "+(address.strip().split(" ")[-1])
            if zipcode:
                item_loader.add_value("zipcode", zipcode.strip())
            
            if "," in address:
                city = address.split(",")[1].strip()
                if not city.count(" ") ==1:
                    city = address.split(",")[1].strip().split(zipcode)[0]
                item_loader.add_value("city", city)
            else: item_loader.add_value("city", "High Wycombe")
            
        rent = response.xpath("normalize-space(//p[@class='property-price']/text())").extract_first()
        if rent:
            price = rent.replace(",","")
            item_loader.add_value("rent_string", price.strip())

        images = [ x for x in response.xpath("//div[@class='carousel-slide']/div/div//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        description = " ".join(response.xpath("//div[contains(@class,'property-content')]/p/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())

        latitude = " ".join(response.xpath("substring-before(substring-after(//script[@class='js-json-map-results']/text(),'latitude'),',')").getall()).strip()   
        if latitude:
            item_loader.add_value("latitude", latitude.replace('":"',"").replace('"','').strip())

        longitude = " ".join(response.xpath("substring-before(substring-after(//script[@class='js-json-map-results']/text(),'longitude'),',')").getall()).strip()   
        if longitude:
            item_loader.add_value("longitude", longitude.replace('":"',"").replace('"','').strip())

        deposit = response.xpath("//span[contains(.,'Deposit')]/text()").get() 
        if deposit:
            deposit = deposit.split("£")[1].strip()
            item_loader.add_value("deposit", deposit)

        if "Semi-Detached" in room:
            item_loader.add_value("furnished", True)
        elif response.xpath("//ul[@class='property-features']/li//text()[contains(.,'Furnished')]").extract_first():
            item_loader.add_value("furnished", True)

        if response.xpath("//span[contains(.,'Parking')]").get():
            item_loader.add_value("parking", True)
        
        item_loader.add_value("landlord_phone", "01494 57 1032")
        item_loader.add_value("landlord_email", "contact@tharaproperties.co.uk")
        item_loader.add_value("landlord_name", "Thara Properties")


        yield item_loader.load_item()