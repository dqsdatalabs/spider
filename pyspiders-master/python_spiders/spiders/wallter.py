# -*- coding: utf-8 -*-
# Author: Rajat Mishra

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
import json
import re


class MySpider(Spider):
    name = "wallter"
    custom_settings = {
        "PROXY_ON": True,
    }
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    external_source = "Wallter_PySpider_belgium_nl"
    def start_requests(self):
        start_urls = [
            {"url": "http://www.wallter.be/nl/te-huur/appartementen", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath(
            "//div[contains(@class,'seven columns')]/a[contains(@class,'half-btn')]/@href"
        ).extract():
            yield Request(item, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        pagination = response.xpath("//ul[@class='pagination']/li[@class='active']/following-sibling::li[@class='arrow']/a/@href").get()
        if pagination:
            url = response.urljoin(pagination)
            yield Request(url, callback=self.parse, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        title = "".join(
            response.xpath("//h1[@class='title color-detail-titles']/text()").extract()
        )
        if title:
            item_loader.add_value("title", title.strip())
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("description", "//div[@class='text-height']")
        price = response.xpath(
            "//div[@class='details four columns price']/text()[contains(., '€')]"
        ).extract_first()
        if price is not None:
            item_loader.add_value("rent", price.replace("€", ""))
            item_loader.add_value("currency", "EUR")

        ref = response.xpath("//div[@class='questions']/p/text()").get()
        ref = ref.split(":")[1]
        item_loader.add_value("external_id", ref)
        item_loader.add_xpath(
            "available_date",
            "//div[@class='row property-details']//div[./h1[.='Financieel']]//li[text()='Beschikbaarheid datum: ' or text()='Beschikbaarheid: ']/span/text()",
        )

        room = "".join(response.xpath("//span[@class='bed-i']/text()").extract())
        if room:
            item_loader.add_value("room_count", room.strip())
        square = "".join(response.xpath("//span[@class='size-i']/text()").extract())
        if len(square) > 1:
            item_loader.add_value("square_meters", square)

        address = "".join(
            response.xpath(
                "//div[contains(@class,'row address')]/div[contains(@class,'eight columns')]/text()"
            ).extract()
        )
        address = re.sub("\s{2,}", " ", address)
        item_loader.add_value("address", address)
        item_loader.add_value("zipcode", split_address(address, "zip"))
        item_loader.add_value("city", split_address(address, "city"))

        terrace = response.xpath(
            "//div[@class='row property-details']//div[./h1[.='Comfort']]//li[contains(.,'Lift')]/span[.='Ja']"
        ).get()
        if terrace:
            if terrace == "Ja":
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[contains(@class,'slick-slide')]/a/@href"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_xpath(
            "energy_label",
            "//div[@class='row property-details']//div[./h1[.='Energie']]//li[text()='EPC score: ']/span/text()",
        )
        phone = response.xpath(
            '//div[@class="four columns address"]/ul/li/div/a/text()'
        ).get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        
        item_loader.add_value("landlord_email", "info@wallter.be")
        item_loader.add_value("landlord_name", "Wallter bvba")
        yield item_loader.load_item()


def split_address(address, get):
    if "," in address:
        temp = address.split(",")[1]
        zip_code = "".join(filter(lambda i: i.isdigit(), temp))
        city = temp.split(zip_code)[1]

        if get == "zip":
            return zip_code
        else:
            return city
