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
    name = 'leasedandsold_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://leasedandsold.com.au/?action=epl_search&post_type=rental&epl_property_status=current&property_category=Apartment",
                    "https://leasedandsold.com.au/?action=epl_search&post_type=rental&epl_property_status=current&property_category=Unit",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://leasedandsold.com.au/?action=epl_search&post_type=rental&epl_property_status=current&property_category=House",
                    "https://leasedandsold.com.au/?action=epl_search&post_type=rental&epl_property_status=current&property_category=Townhouse",
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

        for item in response.xpath("//h3[@class='entry-title']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

        next_button = response.xpath("//a[@rel='Next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Leasedandsold_Com_PySpider_australia")
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("zipcode", "//div[contains(@class,'property-details')]/h1/span/span[@class='item-pcode']/text()")
        item_loader.add_xpath("city", "//div[contains(@class,'property-details')]/h1/span/span[@class='item-suburb']/text()")
        item_loader.add_xpath("room_count", "substring-before(//ul/li[@class='bedrooms']/text(),' ')")
        item_loader.add_xpath("bathroom_count", "substring-before(//ul/li[@class='bathrooms']/text(),' ')")

        rent = response.xpath("normalize-space(//span[@class='page-price-rent']/span[@class='page-price']/text())").extract_first()
        if rent:
            price = rent.replace("$","").strip()
            item_loader.add_value("rent",int(price)*4)
        item_loader.add_value("currency","AUD")

        address = "".join(response.xpath("//div[contains(@class,'property-details')]/h1//text()").extract())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))

        available_date=response.xpath("substring-before(//div[contains(@class,'property-meta')]/div[@class='property-meta date-available']/text(),'at ')").get()
        if available_date:
            date2 =  available_date.replace("Available from","").strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        images = [x for x in response.xpath("//figure/div[@class='gallery-icon landscape']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        description = "".join(response.xpath("//div[contains(@class,'epl-section-description')]/div//text()").extract())
        if description:
            item_loader.add_value("description",description.strip())

        parking = response.xpath("substring-before(//ul/li[@class='parking']/text(),' ')").extract_first()
        if parking:
            item_loader.add_value("parking",True)

        dishwasher = "".join(response.xpath("//ul/li[@class='dishwasher']/text()").extract())
        if dishwasher:
            item_loader.add_value("dishwasher",True)

        balcony = "".join(response.xpath("//ul/li[@class='balcony']/text()").extract())
        if balcony:
            item_loader.add_value("balcony",True)


        item_loader.add_xpath("landlord_name", "normalize-space(//h5[contains(@class,'author-title')]/a/text())")
        item_loader.add_xpath("landlord_phone", "//div[contains(@class,'author-contact')]/span/text()")
        item_loader.add_value("landlord_email", "reception@leasedandsold.com.au")
        yield item_loader.load_item()