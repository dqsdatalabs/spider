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
    name = 'morgan_associates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.morgan-associates.co.uk/search/?instruction_type=Letting&address_keyword=&property_type=Apartment&minprice=&maxprice=",
                    "https://www.morgan-associates.co.uk/search/?instruction_type=Letting&address_keyword=&property_type=Apartment+-+Purpose+Built&minprice=&maxprice=",
                    "https://www.morgan-associates.co.uk/search/?instruction_type=Letting&address_keyword=&property_type=Flat&minprice=&maxprice=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.morgan-associates.co.uk/search/?instruction_type=Letting&address_keyword=&property_type=House&minprice=&maxprice=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    
    def parse(self, response):

        for item in response.xpath("//div[@class='property']/div"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'MORE') and contains(.,'INFORMATION')]/@href").get())
            let_agreed = item.xpath(".//*[name()='text' and contains(.,'AGREED')]").get()
            if not let_agreed: yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

        next_button = response.xpath("//a[@class='next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Morgan_Associates_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("details/")[1].split("/")[0])
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title)
            if " in " in title:
                address = title.split(" in ")[1].split(" - ")[0].strip()
                item_loader.add_value("address", address)
                
                address2 = address.split(" ")
                zipcode = f"{address2[-2]} {address2[-1]}"
                city = address.split(zipcode)[0].strip()
                
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)
                        
        rent = response.xpath("//h1//strong/text()").get()
        if rent:
            price = rent.split(" ")[0].strip().replace(",","")
            item_loader.add_value("rent_string", price)
        
        room_count = response.xpath("//li[contains(.,'BEDROOM')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        bathroom_count = response.xpath("//li[contains(.,'BATHROOM')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        description = " ".join(response.xpath("//div[@id='property-description']//div//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='carousel-inner']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        parking = response.xpath("//li[contains(.,'PARKING')]/text()[not(contains(.,'NO'))]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('&q=')[1].split('%2C')[0]
            longitude = latitude_longitude.split('%2C')[1].split('"')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "Morgan Associates")
        item_loader.add_value("landlord_phone", "01242514285")
        item_loader.add_value("landlord_email", "info@morgan-associates.co.uk")

        yield item_loader.load_item()