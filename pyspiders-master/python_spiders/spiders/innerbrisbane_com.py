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
    name = 'innerbrisbane_com'
    execution_type='testing'
    country='australia'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://innerbrisbane.com/rentals/residential-rentals/?streetsuburbRentals=Street+Suburb+search&minpricer=0&maxpricer=0&bedrooms%5B%5D=&carspaces%5B%5D=&listingtype%5B%5D=Apartment&qt=search&useSearchType=search",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://innerbrisbane.com/rentals/residential-rentals/?streetsuburbRentals=Street+Suburb+search&minpricer=0&maxpricer=0&bedrooms%5B%5D=&carspaces%5B%5D=&listingtype%5B%5D=House&qt=search&useSearchType=search",
                    "https://innerbrisbane.com/rentals/residential-rentals/?streetsuburbRentals=Street+Suburb+search&minpricer=0&maxpricer=0&bedrooms%5B%5D=&carspaces%5B%5D=&listingtype%5B%5D=Townhouse&qt=search&useSearchType=search",
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

        for item in response.xpath("//div[@class='listingImage']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

        next_button = response.xpath("//a[contains(.,'»')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split(",")[-1])
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Innerbrisbane_PySpider_australia")
      
        item_loader.add_xpath("title","//title/text()")
        room_count = response.xpath("//li[span[.='Bedrooms']]/text()").get()
        if room_count:
            if room_count.strip() == "s":
                item_loader.add_value("room_count", "1")
            else:
                item_loader.add_value("room_count", room_count)
        item_loader.add_xpath("bathroom_count","//li[span[.='Bathrooms']]/text()")
        item_loader.add_xpath("deposit","//div[@class='ldHeadline']/span[@class='ldBond']/span/text()")
        rent = response.xpath("//h2[@class='displayPrice']/text()").get()
        if rent and "$" in rent:
            rent = "".join(filter(str.isnumeric, rent.replace(",","")))
            item_loader.add_value("rent", str(int(float(rent)*4)))
        item_loader.add_value("currency", "AUD")
        
        address = response.xpath("//div[@class='ldHeadline']/span[@class='ldAddress']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            city =" ".join(address.split(",")[-1].strip().split(" ")[:-1])
            zipcode = address.split(",")[-1].strip().split(" ")[-1]
            if zipcode.isdigit():
                item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("city", city)
        parking = response.xpath("//li[span[.='Carspaces']]/text()").get()
        if parking:
            if parking.strip() =="0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        balcony = response.xpath("//li[.='Balcony']/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        swimming_pool = response.xpath("//li[.='Inground Pool']/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        dishwasher = response.xpath("//li[.='Dishwasher']/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        furnished = response.xpath("//li[.='Furnished' or .='furnished']/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
     
        description = " ".join(response.xpath("//div[@class='contentItem']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        available_date = response.xpath("//div[@class='ldHeadline']/span[@class='ldAvailable']/span/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='ldGallery']/ul/li/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
    
        item_loader.add_value("landlord_name", "Inner Brisbane Realty")
        item_loader.add_value("landlord_phone", "07 3839 5000")
        item_loader.add_value("landlord_email", "admin@innerbrisbane.com")
        yield item_loader.load_item()