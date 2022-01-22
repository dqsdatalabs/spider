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
    name = 'Capitalestateagents'
    external_source = 'Capitalestateagents_PySpider_united_kingdom'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.capitalestateagents.com/properties/?action=search&type=property&sort=price-highest&per-page=12&view=list&tenure=lettings&location&bedrooms-min=1&sales-price-min=0&lettings-price-min=0&sales-price-max=999999999999&lettings-price-max=999999999999&property-type=apartment",
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

        for item in response.xpath("//a[@class='property__link']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//p[@class='label qualifier-title']//text()[contains(.,'REF')]").get()
        if external_id:
            external_id = external_id.split("REF ")[1]
        item_loader.add_value('external_id', external_id)
  
        title = response.xpath('//title//text()').get()
        if title:
            item_loader.add_value('title', title)
              
        address = response.xpath("//p[contains(@class,'margin-bottom-8 text-primary property__content--address  h4')]//text()").get()
        if address:
            item_loader.add_value('address', address)
            if ","in address:
                city= address.split(",")[1]
                if city:
                    item_loader.add_value('city', city)
     
        room_count = response.xpath("//span[@class='count-lost__pipe']//parent::small[contains(.,'Bed')]/text()").get()
        if room_count:
            room_count = room_count.split("Bed")[0].strip()
            item_loader.add_value('room_count', room_count)

        bathroom_count = response.xpath("//span[@class='count-lost__pipe']//parent::small[contains(.,'Bathrooms')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split("Bathrooms")[0].strip()
            item_loader.add_value('bathroom_count', bathroom_count)

        description = response.xpath("//div[@class='text-primary']//p//text()").getall()
        if description:
            description = " ".join(description)
            item_loader.add_value('description',description)

        rent = response.xpath("//h4[contains(@class,'h1 text-primary')]//text()").get()
        if rent:
            rent = rent.strip()
            rent = rent.split("£")[1]
            if "pcm" in rent.lower():
               rent = rent.split("pcm")[0]
            if "," in rent:
                rent = rent.replace(",","")
            item_loader.add_value('rent', rent)
        item_loader.add_value("currency", "GBP")

        images = response.xpath("//img[contains(@alt,'Property Image')]//@src").extract()
        if images:
            images = list(set(images))
            item_loader.add_value('images', images)

        floor_plan_images = response.xpath("(//img[contains(@alt,'Property Floorplan')]//@data-pagespeed-lazy-src)[1]").extract()
        if floor_plan_images:
            floor_plan_images = list(set(floor_plan_images))
            item_loader.add_value('floor_plan_images', floor_plan_images)
        else:
            floor_plan_images = response.xpath("(//img[contains(@alt,'Property Floorplan')]//@src)[1]").extract()
            if floor_plan_images:
                floor_plan_images = list(set(floor_plan_images))
                item_loader.add_value('floor_plan_images', floor_plan_images)

        item_loader.add_value('landlord_name', "Capital Estate Agents")
        item_loader.add_value('landlord_phone', '0208 295 6101')

        yield item_loader.load_item()