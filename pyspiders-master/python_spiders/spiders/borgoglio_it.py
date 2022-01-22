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
    name = 'borgoglio_it'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = 'Borgoglio_PySpider_italy'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://www.borgoglio.com/milano",
                ],
                "property_type" : "apartment"
            }
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

        for item in response.xpath("//a[@class='simple-ajax-popup-align-top']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        status = response.xpath("//div[@class='info-text']/span[contains(.,'Tipologia')]/following-sibling::span/text()").get()
        if status and ("vendita" in status.lower() or "commerciale" in status.lower()):
            return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//div[@class='project-info']/h2/text()").get()
        if title:
            item_loader.add_value("title", title)
        external_id = response.url
        if external_id:
            item_loader.add_value("external_id", external_id.split('node/')[-1])

        rent = response.xpath("//div[@class='info-text']/span[contains(.,'Richiesta')]/following-sibling::span/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace(".","").strip())
        
        utilities = response.xpath("//div[@class='info-text']/span[contains(.,'condominiali')]/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace(".","").strip())
        
        item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//div[@class='info-text']/span[contains(.,'Mq')]/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//div[@class='info-text']/span[contains(.,'Camere')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[@class='info-text']/span[contains(.,'Bagni')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        address = "".join(response.xpath("//div[@class='project-info']/h2/text()").getall())
        if address:
            item_loader.add_value("address", address.split('Via')[-1].strip())
            item_loader.add_value("city", "Milano")
        
        floor = response.xpath("//div[@class='info-text']/span[contains(.,'Piano')]/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor", floor)

        energy_label = response.xpath("//div[@class='info-text']/span[contains(.,'energetica')]/following-sibling::span/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.upper())
        
        images = [x for x in response.xpath("//div[@class='col-sm-8 project-images ']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        item_loader.add_value("landlord_name", "Borgoglio")
        item_loader.add_value("landlord_phone", "+390243912794")
        item_loader.add_value("landlord_email", "info@borgoglio.com")

        yield item_loader.load_item()