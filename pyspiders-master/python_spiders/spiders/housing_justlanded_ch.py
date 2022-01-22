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
    name = 'housing_justlanded_ch'
    external_source = "Housing_Justlanded_PySpider_ireland"
    execution_type='testing'
    country='ireland'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://housing.justlanded.ch/en/World/For-Rent_Apartments",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://housing.justlanded.ch/en/World/For-Rent_Houses"
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
        
        page = response.meta.get('page', 2)
        property_type=response.meta.get('property_type')
        seen = False
        for item in response.xpath("//a[@class='d-inline-block check-for-keywords pr-5 pr-md-1']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        if property_type and "apartment" in property_type:
            if page == 2 or seen:
                url = f"https://housing.justlanded.ch/en/World/For-Rent_Apartments/{page}"
                yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})
        else:
             if page == 2 or seen:
                url = f"https://housing.justlanded.ch/en/World/For-Rent_Houses/{page}"
                yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)

        address = response.xpath("//span[@itemprop='address']/text()").get()
        if address:
            item_loader.add_value("address",address)

        description = response.xpath("//div[@itemprop='description']/ul/li/text()").getall()
        if description:
            item_loader.add_value("description",description)

        rent = response.xpath("//span[@class='source-price d-inline-block mr-2']/text()").get()
        if rent:
            if rent and "," in rent:
                rent = rent.replace(",","")
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","KWD")
        
        room_count = response.xpath("//div[@class='bedrooms clearfix']/span[@itemprop='numberOfRooms']/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        images = [response.urljoin(x) for x in response.xpath("//img[@class='img-fluid owl-lazy']//@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images)),

        item_loader.add_value("landlord_name","housing.justlanded")
        item_loader.add_value("landlord_phone","+965 98 055 838")

        yield item_loader.load_item()