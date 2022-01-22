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
    name = 'mackayrentals_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    
    custom_settings = {
        "PROXY_ON" : "True",
    }
    external_source = "Mackayrentals_Com_PySpider_australia"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://mackayrentals.com.au/page/1/?action=epl_search&post_type=rental&property_status=current&property_category=Unit",
                    "https://mackayrentals.com.au/page/1/?action=epl_search&post_type=rental&property_status=current&property_category=Apartment",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://mackayrentals.com.au/page/1/?action=epl_search&post_type=rental&property_status=current&property_category=House",
                    "https://mackayrentals.com.au/page/1/?action=epl_search&post_type=rental&property_status=current&property_category=Townhouse",
                    "https://mackayrentals.com.au/page/1/?action=epl_search&post_type=rental&property_status=current&property_category=Villa",
                    "https://mackayrentals.com.au/page/1/?action=epl_search&post_type=rental&property_status=current&property_category=DuplexSemi-detached",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='property-address']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//div[@class='epl-pagination']//a[@rel='next']/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source )    
        item_loader.add_xpath("title","//div[@class='inside-page-hero grid-container grid-parent']/text()[normalize-space()]")
  
        address =response.xpath("//div[@class='inside-page-hero grid-container grid-parent']/text()[normalize-space()]").get()
        if address:
            item_loader.add_value("address", address )
            address = address.split(",")[1].replace("  "," ")
            zipcode = address.split(" ")[-2]+" "+address.split(" ")[-1]
            city = address.replace(zipcode,"")
            item_loader.add_value("city", city.strip() )
            item_loader.add_value("zipcode", zipcode.strip()  )
 
        item_loader.add_xpath("room_count", "//span[@title='Bedrooms']/span/text()")
        item_loader.add_xpath("bathroom_count", "//span[@title='Bathrooms']/span/text()")

        rent = " ".join(response.xpath("//span[@class='page-price-rent']/span/text()").getall())
        if rent:
            rent = rent.split("$")[-1].lower()
            if "week" in rent:
                rent = rent.split("/")[0].split("per")[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
            else:       
                rent = rent.split("/")[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'AUD')

        deposit =response.xpath("//span[@class='bond']/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(",","") )
        available_date = response.xpath("//div[contains(@class,'date-available')]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Available from")[-1].split(",")[-1], date_formats=["%d %m %Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        description = " ".join(response.xpath("//div[contains(@class,'epl-section-description')]/div/p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        images = [x for x in response.xpath("//div[@id='epl-slider-slides']/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        latlng = response.xpath("//div/@data-cord").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
        parking = response.xpath("//span[@title='Parking Spaces']/span/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        balcony = response.xpath("//li[.='Balcony']/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        item_loader.add_value("landlord_name", "Mackay Rentals")
        item_loader.add_value("landlord_phone", "07 4944 0222")
        item_loader.add_value("landlord_email", "rentals@mackayrentals.com.au")

        yield item_loader.load_item()