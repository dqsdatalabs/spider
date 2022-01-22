# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import lxml, js2xml
class MySpider(Spider):
    name = 'homelocators_ie'
    execution_type='testing'
    country='ireland'
    locale='en'
    external_source = "Homelocators_PySpider_ireland"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.homelocators.ie/wp-admin/admin-ajax.php?action=wppd_property_fetch&payload={%22page%22:1,%22price_min%22:%220%22,%22price_max%22:%22995000%22,%22property_market%22:%22residential%22,%22property_status%22:%22To%20Let,%20Let,%20Let%20Agreed,%20Has%20Been%20Let%22,%22type%22:%22apartment%22,%22location%22:[]}",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.homelocators.ie/wp-admin/admin-ajax.php?action=wppd_property_fetch&payload={%22page%22:1,%22price_min%22:%220%22,%22price_max%22:%22995000%22,%22property_market%22:%22residential%22,%22property_status%22:%22To%20Let,%20Let,%20Let%20Agreed,%20Has%20Been%20Let%22,%22type%22:%22house%22,%22location%22:[]}"
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
        
        seen = False
        for item in response.xpath("//div[contains(@class,'property-card--image')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.era.be/nl/te-huur?page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title = "".join(response.xpath("//title//text()").get())
        if title:
            item_loader.add_value("title",title)

        address = "".join(response.xpath("normalize-space(//script[@type='application/ld+json']/text()[contains(.,'address')])").extract())
        if address:
            latitude = address.split('latitude":')[-1].split(',')[0].strip().replace('"','')
            longitude = address.split('longitude":')[-1].split('}')[0].strip().replace('"','').replace('\n','')
            address = address.split('streetAddress":')[-1].split(',')[0].strip().replace('"','')
            if address:
                item_loader.add_value("address", address)
            if latitude:
                item_loader.add_value("latitude", latitude)
            if longitude:
                item_loader.add_value("longitude", longitude)
                
        square_meters = response.xpath("//div[contains(@class,'grid-property-attribute grid-property-attribute-size flex-element')]//following-sibling::em//text()").get()
        if square_meters:
            square_meters=square_meters.split("sq.m.")[0]
            item_loader.add_value("square_meters",square_meters)

        description = "".join(response.xpath("//div[contains(@id,'property-description')]//p//text()").getall())
        if description:
            item_loader.add_value("description",description)

        furnished = response.xpath("//div[contains(@id,'property-features')]/ul/li/text()[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished",True)

        parking = response.xpath("//div[contains(@id,'property-features')]/ul/li/text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking",True)

        terrace = response.xpath("//div[contains(@id,'property-features')]//following-sibling::li//text()").get()
        if 'Terrace' in terrace:
            item_loader.add_value("terrace",True)

        balcony = response.xpath("//div[contains(@id,'property-features')]//following-sibling::li//text()").get()
        if 'Balcony' in balcony:
            item_loader.add_value("balcony",True)

        rent = response.xpath("//div[contains(@class,'grid-property-attribute grid-property-attribute-price flex-element')]//following-sibling::em//text()").get()
        if rent:
            rent=rent.split("€")[1].replace(",",".")
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        room_count = response.xpath("//div[contains(@class,'grid-property-attribute grid-property-attribute-bedrooms flex-element')]//following-sibling::em//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count = response.xpath("//div[contains(@class,'grid-property-attribute grid-property-attribute-bathrooms flex-element')]//following-sibling::em//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        images = [response.urljoin(x)for x in response.xpath("//img[contains(@loading,'lazy')]//@src").getall()]
        if images:
                item_loader.add_value("images",images)



        item_loader.add_value("landlord_phone", "+353 1 679 5233 / 14")
        item_loader.add_value("landlord_email", "info@homelocators.ie")
        item_loader.add_value("landlord_name", "Home Locators")






        yield item_loader.load_item()