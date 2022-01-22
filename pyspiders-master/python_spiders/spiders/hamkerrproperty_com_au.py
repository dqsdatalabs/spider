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
    name = 'hamkerrproperty_com_au'    
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.hamkerrproperty.com.au/search-properties-for-lease/?sortby=created-desc&min_price=&max_price=&per_page=18&surrounding=&category%5B%5D=Unit&category%5B%5D=Apartment&min_bed=&max_bed=&min_bath=&max_bath=&min_parking_total=&max_parking_total=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.hamkerrproperty.com.au/search-properties-for-lease/?sortby=created-desc&min_price=&max_price=&per_page=18&surrounding=&category%5B%5D=House&category%5B%5D=Townhouse&min_bed=&max_bed=&min_bath=&max_bath=&min_parking_total=&max_parking_total=",
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

        for item in response.xpath("//div[@class='property']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

        next_button = response.xpath("//a[@aria-label='Next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("-")[0].split("/")[-1])
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Hamkerrproperty_Com_PySpider_australia")
        item_loader.add_xpath("title","//title/text()")
        city = response.xpath("//dt[.='Location']/following-sibling::dd[1]//text()").get()
        if city:
            item_loader.add_value("city", city.split(",")[0].strip())
        zipcode = response.xpath("//title/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", "".join(zipcode.split(',')[-2:]))
        address = " ".join(response.xpath("//header[@class='property-title']//text()[normalize-space()]").getall())
        if address:
            item_loader.add_value("address", address)
        item_loader.add_xpath("room_count","//dt[.='Beds:']/following-sibling::dd[1]//text()")
        item_loader.add_xpath("bathroom_count","//dt[.='Baths:']/following-sibling::dd[1]//text()")
        rent = response.xpath("//dt[.='Price']/following-sibling::dd[1]//text()").get()
        if rent:
            rent = "".join(filter(str.isnumeric, rent.replace(",","")))
            item_loader.add_value("rent", str(int(float(rent)*4)))
        item_loader.add_value("currency", "AUD")
        available_date = response.xpath("//dt[.='Available']/following-sibling::dd[1]//text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        parking = response.xpath("//dt[.='Parking:']/following-sibling::dd[1]//text()").get()
        if parking:
            if parking.strip() =="0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        balcony = response.xpath("//li[.='Balcony']/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
   
        dishwasher = response.xpath("//li[.='Dishwasher']/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
   
        furnished = response.xpath("//li[.='Furnished' or .='furnished']/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
     
        description = " ".join(response.xpath("//section[@id='description']/text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        latlng = response.xpath("//script[contains(.,'lat: ')]/text()").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split("lat: ")[1].split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split("lng: ")[1].split("}")[0].strip())
        images = [x for x in response.xpath("//div[@id='slick-gallery']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
     
        item_loader.add_xpath("landlord_name", "//div[@class='agent-contact-info']/h3/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='agent-contact-info']//dt[.='Phone:']/following-sibling::dd[1]//text()")
        item_loader.add_value("landlord_email", "mail@hamkerrproperty.com.au")
        yield item_loader.load_item()