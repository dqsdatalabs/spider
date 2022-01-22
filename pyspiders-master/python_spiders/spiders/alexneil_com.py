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
import dateparser
from word2number import w2n

class MySpider(Spider):
    name = 'alexneil_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):

        start_urls = [
            {
                "type" : "House",
                "property_type" : "house"
            },
            {
                "type" : "Flat",
                "property_type" : "apartment"
            },
            {
                "type" : "Duplex",
                "property_type" : "apartment"
            },
            {
                "type" : "Maisonette",
                "property_type" : "apartment"
            },
            {
                "type" : "Studio",
                "property_type" : "studio"
            },
            {
                "type" : "Terraced",
                "property_type" : "house"
            },
        ]
        for url in start_urls:

            formdata = {
                "propsearchtype": "",
                "searchurl": "/rental-property-search",
                "market": "1",
                "ccode": "UK",
                "pricetype": "3",
                "view": "",
                "postcodes": "",
                "pricelow": "",
                "pricehigh": "",
                "propbedr": "",
                "proptype": url.get("type"),
                "statustype": "2",
            }

            yield FormRequest(
                url="https://www.alexneil.com/results",
                callback=self.parse,
                formdata=formdata,
                meta={'property_type': url.get('property_type')}
            )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(@class,'more-info')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        prop_type = response.meta.get('property_type')

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Alexneil_PySpider_united_kingdom")

        external_id = response.xpath("//p[contains(.,'Property Reference')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[-1].strip())

        address = response.xpath("//div[@class='detail-contact']/../h3/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(',')[-1].strip())
            item_loader.add_value("city", address.split(',')[-2].strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

            if "maisonette" in title.lower():
                prop_type = "apartment"
        
        item_loader.add_value("property_type", prop_type)
        
        description = " ".join(response.xpath("//div[@class='detail-prop-content']/p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

            if 'sq ft' in description:
                square_meters = description.split('sq ft')[0].strip().split(' ')[-1].replace(',', '')
                if square_meters.isnumeric():
                    item_loader.add_value("square_meters", str(int(int(square_meters) * 0.09290304)))

        room_count = response.xpath("//div[@class='bullets-li']//text()[contains(.,'Bedroom') or contains(.,'bedroom')]").get()
        if room_count:
            try:
                item_loader.add_value("room_count", w2n.word_to_num(room_count.lower().split('bedroom')[0].strip()))
            except:
                pass
        else:
            room_count = response.xpath('//h2[contains(.,"Bedroom")]/text()').re(r'\d')
            if room_count:
                item_loader.add_value("room_count", room_count[0])
        
        bathroom_count = response.xpath("//div[@class='bullets-li']//text()[contains(.,'Bathroom') or contains(.,'bathroom')]").get()
        if bathroom_count:
            try:
                item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count.lower().split('bathroom')[0].strip()))
            except:
                pass

        rent = response.xpath("//h1/span[@class='priceask']/text()").get()
        if rent:
            rent = rent.split('£')[-1].split('pcm')[0].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'GBP')
        
        deposit = response.xpath("//div[@class='bullets-li']//text()[contains(.,'Deposit')]").get()
        if deposit:
            item_loader.add_value("deposit", str(int(float(deposit.split('£')[-1].strip().replace(',', '')))))
        
        images = [response.urljoin(x) for x in response.xpath("//ul[@class='slides-container']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='floorplan-slider']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//script[contains(.,'alexneil.com/detail')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('latitude":"')[1].split('"')[0].strip())
            item_loader.add_value("longitude", latitude.split('longitude":"')[1].split('"')[0].strip())
        
        energy_label = response.xpath("//td[@class='epcCurrent']/img[contains(@src,'energy')]/@src").get()
        if energy_label:
            energy_label = int(energy_label.split('/')[-1].split('.')[0].strip())
            if energy_label >= 92:
                item_loader.add_value("energy_label", 'A')
            elif energy_label >= 81 and energy_label <= 91:
                item_loader.add_value("energy_label", 'B')
            elif energy_label >= 69 and energy_label <= 80:
                item_loader.add_value("energy_label", 'C')
            elif energy_label >= 55 and energy_label <= 68:
                item_loader.add_value("energy_label", 'D')
            elif energy_label >= 39 and energy_label <= 54:
                item_loader.add_value("energy_label", 'E')
            elif energy_label >= 21 and energy_label <= 38:
                item_loader.add_value("energy_label", 'F')
            elif energy_label >= 1 and energy_label <= 20:
                item_loader.add_value("energy_label", 'G')
        
        floor = response.xpath("//div[@class='bullets-li']//text()[contains(.,'Floor') or contains(.,'floor')]").get()
        if floor:
            floor = "".join(filter(str.isnumeric, floor.lower().split('floor')[0].strip()))
            if floor:
                item_loader.add_value("floor", floor)
        
        parking = response.xpath("//div[@class='bullets-li']//text()[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[@class='bullets-li']//text()[contains(.,'Balcony') or contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//div[@class='bullets-li']//text()[contains(.,'Furnished') or contains(.,'furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        terrace = response.xpath("//div[@class='bullets-li']//text()[contains(.,'Terrace') or contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_name", "Alex Neil Estate Agents")
        item_loader.add_value("landlord_phone", "020 7237 6767")
        item_loader.add_value("landlord_email", "sl@alexneil.com")
        
        yield item_loader.load_item()
