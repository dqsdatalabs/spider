# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy

class KnightsbridgeEstateAgentsSpider(scrapy.Spider):
    name = 'knightsbridge_estate_agents'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="knightsbridgeestateagents_PySpider_united_kingdom_en"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://knightsbridge-estates.co.uk/property/?department=residential-lettings&property_type=Apartment&minimum_bedrooms=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&keyword=",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://knightsbridge-estates.co.uk/property/?department=residential-lettings&property_type=House&minimum_bedrooms=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&keyword="
                ],
                "property_type": "house"
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

        for item in response.xpath("//a[@class='listing-featured-thumb']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//a[contains(@class,'label-status')]//text()").get()
        if status and "let agreed" in status.lower():
            return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        external_id = "".join(response.xpath("//link[@rel='shortlink']//@href").get())
        if external_id:
            if "=" in external_id:
                external_id = external_id.split("=")[1]
            item_loader.add_value("external_id",external_id)

        address = response.xpath("(//address[@class='item-address']//text())[1]").get()
        if address:
            item_loader.add_value("address",address)

        description = response.xpath("//ul[@class='list-2-cols list-unstyled']//text()").getall()
        if description:
            item_loader.add_value("description",description)

        rent = response.xpath("(//li[@class='item-price']//text())[1]").get()
        if rent:
            rent = rent.replace(" ","")
            rent = rent.split("£")[1].split("/pcm")[0]
            if "," in rent:
                rent = rent.replace(",","") 
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","GBP")

        room_count = response.xpath("//i[@class='houzez-icon icon-hotel-double-bed-1 mr-1']//following-sibling::strong//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        
        bathroom_count = response.xpath("//i[@class='houzez-icon icon-bathroom-shower-1 mr-1']//following-sibling::strong//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        deposit = response.xpath("//input[@placeholder='Deposit']//@value").get()
        if deposit:
            item_loader.add_value("deposit",deposit)

        longitute_latitude = response.xpath("//script[@id='houzez-custom-js-extra']//text() ").get()
        if longitute_latitude:
            latitude = longitute_latitude.split('"default_lat":"')[1].split('",')[0]
            longitude=longitute_latitude.split('"default_long":"')[1].split('",')[0]
            item_loader.add_value("latitude",latitude)
            item_loader.add_value("longitude",longitude)
           
        images = [x for x in response.xpath("//img[@class='img-fluid']//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        landlord_name = response.xpath("(//li[@class='agent-name']/text())[1]").get()
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name)
        else:
            item_loader.add_value("landlord_name","Knightsbridge Estates")

        landlord_phone = response.xpath("(//span[@class='agent-phone']/a/text())[1]").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone",landlord_phone)
        else:
            item_loader.add_value("landlord_phone","0116 274 5544")

        item_loader.add_value("landlord_email","lettings@knightsbridge-lettings.co.uk")


        yield item_loader.load_item()