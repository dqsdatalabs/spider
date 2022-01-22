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
    name = 'bairstoweves_co_uk'
    execution_type='testing'
    country='united_kingdom' 
    locale='en'
    external_source="Bairstoweves_Co_PySpider_united_kingdom"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.bairstoweves.co.uk/rent/search/page-1/pricing-monthly/flat/exclude-letagreed/",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.bairstoweves.co.uk/rent/search/page-1/pricing-monthly/house/exclude-letagreed/",
                    "https://www.bairstoweves.co.uk/rent/search/page-1/pricing-monthly/bungalow/exclude-letagreed/"
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
        for item in response.xpath("//div[@class='card']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            if "search" not in follow_url:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = response.url.replace(f"page-{page-1}", f"page-{page}")
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.url.split("ref-")[1].split("/")[0])

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        address_rent = response.xpath("//script[contains(@id,'ng-details-panel')]//text()").get()
        sel = Selector(text=address_rent, type='html')
        room_count = sel.xpath("//li[contains(@class,'details-panel__spec-list-item')]//title[contains(.,'Bedroom')]//parent::svg//parent::div//following-sibling::span[contains(@class,'details-panel__spec-list-number')]//text()").get()
        if room_count and room_count > "0":
            item_loader.add_value("room_count",room_count)
        bathroom_count = sel.xpath("//li[contains(@class,'details-panel__spec-list-item')]//title[contains(.,'Bathroom')]//parent::svg//parent::div//following-sibling::span[contains(@class,'details-panel__spec-list-number')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        address = sel.xpath("//span[contains(@class,'details-panel__title-main')]//text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(",")[-1]
            city = address
            if city.count(",") == 2:
                city = city.split(",")[-2].strip()
            else:
                city = city.replace(",","").strip()
            item_loader.add_value("city",city)
            item_loader.add_value("zipcode",zipcode.strip())

        rent_pw = ""
        rent = " ".join(sel.xpath("//p[contains(@class,'details-panel__details-text-primary')]//text()").getall())
        if rent:
            price = rent.split("(")[1].strip()
            if "pw" in price:
                rent_pw = price.split("pw")[0].replace("£","")
            else:
                rent_pw = rent.split("£")[1].strip().split(" ")[0].replace(",","").strip()
            item_loader.add_value("rent", int(rent_pw)*4)
        item_loader.add_value("currency", "GBP")

        images = response.xpath("//script[contains(@id,'ng-primary-panel')]//text()").get()
        sel_image = Selector(text=images, type='html')
        images = [x for x in sel_image.xpath("//div[contains(@class,'carousel ')]//img[contains(@class,'carousel__image-content')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        desc = " ".join(response.xpath("//h3[contains(@class,'h3--break')][contains(.,'property')]//following-sibling::div//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        parking = response.xpath("//h3[contains(@class,'h3--break')][contains(.,'property')]//following-sibling::div//p//text()[contains(.,'Garage') or contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//h3[contains(@class,'h3--break')][contains(.,'property')]//following-sibling::div//p//text()[contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//h3[contains(@class,'h3--break')][contains(.,'property')]//following-sibling::div//p//text()[contains(.,'Furnished')][not(contains(.,'Unfurnished'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        energy_label = response.xpath("//h3[contains(@class,'h3--break')][contains(.,'property')]//following-sibling::div//p//text()[contains(.,'Epc Rating')]").get()
        if energy_label:
            energy_label = energy_label.strip().split(" ")[-1]
            item_loader.add_value("energy_label", energy_label)

        latitude_longitude = response.xpath("//div[contains(@class,'details-panel')]//@data-location").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat:')[1].split(',')[0]
            longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//h3[contains(@class,'h3--break')][contains(.,'Agent')]//following-sibling::div//h4//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone = response.xpath("//h3[contains(@class,'h3--break')][contains(.,'Agent')]//following-sibling::div//a[contains(@href,'tel')]//@href").get()
        if landlord_phone:
            landlord_phone = landlord_phone.split(":")[1]
            item_loader.add_value("landlord_phone", landlord_phone)

        item_loader.add_value("landlord_email", "privacy@countrywide.co.uk.")


        yield item_loader.load_item()