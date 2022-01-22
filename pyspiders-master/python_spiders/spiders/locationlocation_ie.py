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
    name = 'locationlocation_ie'
    execution_type='testing'
    country='ireland'
    locale='en'
    external_source = "Locationlocation_PySpider_ireland_en"

    def start_requests(self):
        start_urls = [
	        {
                "url": [
                    "https://www.locationlocation.ie/?action=epl_search&post_type=rental"
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
        
        for item in response.xpath("//div[@class='property-box property-featured-image-wrapper']//a//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//span[@class='page-price sold-status']//text()").get()
        if status:
            return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        
        description = response.xpath("//div[@class='epl-tab-section']//p//text()").getall()
        if description:
            item_loader.add_value("description",description)

        address = response.xpath("(//div[@class='epl-tab-section']//p)[1]//text()").get()
        if address:
            item_loader.add_value("address",address)

        rent = response.xpath("//span[@class='page-price']//text()").get()
        if rent:
            rent = rent.split("€")[1].split(" pcm")[0]
            if rent and "," in rent:
                rent =rent.replace(",","")
            rent =rent.replace(" ","") 
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        room_count = response.xpath("//span[@title='Bedrooms']//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        bathroom_count = response.xpath("//span[@title='Bathrooms']//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        furnished = response.xpath("//h2[@class='entry-title']//text()").get()
        if furnished and "unfurnished" in furnished.lower():
            item_loader.add_value("furnished",False)
        else:
            item_loader.add_value("furnished",True)

        images = [response.urljoin(x) for x in response.xpath("//img[contains(@class,'portfolio-image')]//@src").extract()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "location location")
        item_loader.add_value("landlord_phone", "+353 65672 9999")
        item_loader.add_value("landlord_email", "info@locationlocation.ie")

        yield item_loader.load_item()