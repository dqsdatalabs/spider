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
    name = 'fothergillwyatt_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.fothergillwyatt.co.uk/properties/?location=&find=&listingtype=rent&propertytype=flat&bedrooms=",
                    "https://www.fothergillwyatt.co.uk/properties/?location=&find=&listingtype=rent&propertytype=apartment&bedrooms=",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.fothergillwyatt.co.uk/properties/?location=&find=&listingtype=rent&propertytype=house&bedrooms=",
                    "https://www.fothergillwyatt.co.uk/properties/?location=&find=&listingtype=rent&propertytype=coachhouse&bedrooms=",
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

        items = response.xpath("//div[@class='summary']/h4/a")
        for item in items:
            url = item.xpath("./@href").extract_first()
            yield Request(url,
                            callback=self.populate_item,
                            meta={'property_type': response.meta.get("property_type")})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        if response.url.split("/")[-2].isdigit():
            item_loader.add_value("external_id", response.url.split("/")[-2])
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Fothergillwyatt_Co_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("address", "//div[@id='property-single-header']/div/h5/text()")
        item_loader.add_value("city", item_loader.get_collected_values("address")[0].split(',')[-1].strip())

        rent = response.xpath("//div[@class='price']/h2/text()").extract_first()
        if rent:
            price = rent.replace(",","")
            item_loader.add_value("rent_string",price.split(" ")[0].strip())

        room_count = "".join(response.xpath("//ul[@class='share_social property-features-icons']/li[contains(.,'Bedrooms')]/text()").extract())
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0].strip())

        deposit = "".join(response.xpath("substring-before(substring-after(//div[@id='full-details']/div[@class='paragraphs']/p/text()[contains(.,'Deposit')],': '),'.')").extract())
        if deposit:
            item_loader.add_value("deposit",deposit.strip())

        bathroom_count = "".join(response.xpath("//ul[@class='share_social property-features-icons']/li[contains(.,'Bathroom')]/text()").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip().split(" ")[0].strip())

        energy_label = "".join(response.xpath("//ul[@class='share_social property-features-icons']/li[contains(.,' Energy Efficiency Rating:')]/strong/text()").extract())
        if energy_label:
            item_loader.add_value("energy_label",energy_label.strip())       

        description = " ".join(response.xpath("//div[@class='column small-12 medium-12 wide-7 text']//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)

        item_loader.add_xpath("latitude", "substring-before(substring-after(//div[@id='map-view']/iframe/@src,'&q='),',')")
        item_loader.add_xpath("longitude", "substring-before(substring-after(substring-after(//div[@id='map-view']/iframe/@src,'&q='),','),'&')")

        images = [x for x in response.xpath("//div[@class='innerwarpper']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        elevator = "".join(response.xpath("//div[@class='column small-12 medium-12 wide-7 text']/ul/li[contains(.,'Lift')]/text()").extract())
        if elevator:
            item_loader.add_value("elevator", True)

        parking = "".join(response.xpath("//div[@class='column small-12 medium-12 wide-7 text']/ul/li[contains(.,'Parking') or contains(.,'parking')]/text()").extract())
        if parking:
            item_loader.add_value("parking", True)

        furnished = "".join(response.xpath("//div[@class='column small-12 medium-12 wide-7 text']/ul/li[contains(.,'Furnished')]/text()").extract())
        if furnished:
            item_loader.add_value("furnished", True)

        balcony = "".join(response.xpath("//div[@class='column small-12 medium-12 wide-7 text']/ul/li[contains(.,'balcony')]/text()").extract())
        if balcony:
            item_loader.add_value("balcony", True)

        item_loader.add_value("landlord_name", "Fothergill Wyatt")
        item_loader.add_value("landlord_phone", "0116 270 5900")
        item_loader.add_value("landlord_email", "info@fothergillwyatt.com")




        yield item_loader.load_item()