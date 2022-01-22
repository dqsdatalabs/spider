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
    name = 'immobiliaremondani_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Immobiliaremondani_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://immobiliaremondani.it/affitto",
                ],
                "property_type": "apartment"
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
        
        for item in response.xpath("//h3[@class='entry-title']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        if page == 2 or seen:
            url = f"https://immobiliaremondani.it/affitto/page/{page}/?pagination_id=1"
            yield Request(
                url,
                callback=self.parse,
                meta={"page": page+1, "property_type": response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath(
            "//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        description = response.xpath(
            "//div[@class='tab-content']//p//text()").getall()
        if description:
            item_loader.add_value("description", description)
        dontallow=item_loader.get_output_value("description")
        if dontallow and "vendita" in dontallow:
            return 
        city=response.xpath("//h3[@class='secondary-heading']//span[@class='suburb']/text()").get()
        city2=response.xpath("//h3[@class='secondary-heading']//span[@class='state']/text()").get()
        if city and city2:
            item_loader.add_value("city",city2+city)


        bathroom_count = response.xpath(
            "//div[@class='property-feature-icons epl-clearfix']//span[@title='Bagni']//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Bagno")[0])
        room_count=response.xpath("//li[@class='bedrooms']/text()").get()
        if room_count:
            room_count=room_count.split("letto")[0]
            item_loader.add_value("room_count",room_count)

        rent = response.xpath("//p[contains(.,'Prezzo')]//text()").get()
        if rent:
            rent=rent.split("Prezzo")[1].split("euro")[0].split(",")[0].replace(".","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        latitude=response.xpath("//div[@class='epl-tab-section epl-section-map']/div/@data-cord").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split(",")[0])
        longitude=response.xpath("//div[@class='epl-tab-section epl-section-map']/div/@data-cord").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split(",")[-1])
        furnished=response.xpath("//li[@class='furnished']").get()
        if furnished:
            item_loader.add_value("furnished",True)
        available_date=response.xpath("//u/text()").get()
        if available_date:
            available_date=available_date.split("dal")[-1].strip()
            if re.findall("\d+",available_date):
                item_loader.add_value("available_date",available_date)

        square_meters = response.xpath(
            "//ul[@class='epl-property-features listing-info epl-tab-2-columns']//li[@class='building-size']//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("Superficie è")[1].split("m")[0])

        images = [response.urljoin(x) for x in response.xpath(
            "//div[@id='gallery-1']//dt[@class='gallery-icon landscape']//a//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        floor_plan_images = [response.urljoin(x) for x in response.xpath(
            "//div[@id='gallery-1']//dt[@class='gallery-icon portrait']//a//img//@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        item_loader.add_value("landlord_name", "Immobiliare Mondani")
        item_loader.add_value("landlord_phone", "02 20240283")
        item_loader.add_value("landlord_email", "segreteria@immobiliaremondani.it")

        yield item_loader.load_item()