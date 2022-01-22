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
    name = 'joyceproperty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source="Joyceproperty_Com_PySpider_australia"

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.joyceproperty.com.au/?action=epl_search&post_type=rental&property_category=Apartment",
                   
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.joyceproperty.com.au/?action=epl_search&post_type=rental&property_category=House",
                    "https://www.joyceproperty.com.au/?action=epl_search&post_type=rental&property_category=Villa",
                    "https://www.joyceproperty.com.au/?action=epl_search&post_type=rental&property_category=Townhouse",
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

        for item in response.xpath("//a[@class='entry-link']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='page-link']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)          
        item_loader.add_xpath("title","//title/text()")

        rent=response.xpath("//div[@class='property-meta property-price h4']/span/text()").get()
        if rent:
            rent=rent.split("$")[1]
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency", "USD")

        room_count=response.xpath("//div[@class='property-meta property-bedroom h4']/div[@class='meta-thumb']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//div[@class='property-meta property-bathroom h4']/div[@class='meta-thumb']/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        parking=response.xpath("//div[@class='property-meta property-carspaces h4']/div[@class='meta-thumb']/following-sibling::span/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        adres=response.xpath("//h1[@class='entry-title h2']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        zipcode=response.xpath("//h1[@class='entry-title h2']/text()").get()
        if zipcode: 
            item_loader.add_value("zipcode",zipcode.split("  ")[-2:])
        external_id=response.xpath("//span[@class='label property-id']/parent::div/following-sibling::div/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        description=" ".join(response.xpath("//h2[@class='epl-tab-title']/following-sibling::div//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        images=[x.split("(")[1].split(")")[0] for x in response.xpath("//div[@class='splide__list']//div//@style").getall()]
        if images:
            item_loader.add_value("images",images)



        item_loader.add_value("landlord_email", "mail@joyceproperty.com.au")

        yield item_loader.load_item()