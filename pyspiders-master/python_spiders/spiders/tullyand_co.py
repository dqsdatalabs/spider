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
    name = 'tullyand_co'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    custom_settings = {
        "PROXY_ON" : True
    }

    def start_requests(self):
        start_urls = [
            {"url": "https://www.tullyand.co/search.ljson?channel=lettings&fragment=tag-flat", "property_type": "apartment"},
            {"url": "https://www.tullyand.co/search.ljson?channel=lettings&fragment=tag-maisonette", "property_type": "apartment"},
	        {"url": "https://www.tullyand.co/search.ljson?channel=lettings&fragment=tag-house", "property_type": "house"},
            {"url": "https://www.tullyand.co/search.ljson?channel=lettings&fragment=tag-studio", "property_type": "studio"},
            {"url": "https://www.tullyand.co/search.ljson?channel=lettings&fragment=tag-bungalows", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        data = json.loads(response.body)
        page = response.meta.get('page', 2)

        if data["properties"]:
            for item in data["properties"]:
                follow_url = response.urljoin(item["property_url"])
                lat = item["lat"]
                lng = item["lng"]
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type, "lat":lat, "lng":lng})
            
            if data["pagination"]["has_next_page"]:
                base_url = response.meta.get("base_url", response.url)
                url = base_url + f"/page-{page}"
                yield Request(url, callback=self.parse, meta={"page": page+1, "base_url":base_url, "property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Tullyand_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "normalize-space(//h1[@class='property-name']/text()[1])")

        lat = response.meta.get("lat")
        lng = response.meta.get("lng")
        if lat:
            item_loader.add_value("latitude", str(lat).strip())
        if lng:
            item_loader.add_value("longitude", str(lng).strip())

        external_id = response.url.split('/')[-2].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        address = response.xpath("//h1[@class='property-name']/text()[1]").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(',')[-1].strip())
        
        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//text()[contains(.,'Start contract')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split('Start contract')[1].split('(')[0].split('.')[0].strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        description = " ".join(response.xpath("//div[@id='shorten_description']/p//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        room_count = response.xpath("//li[contains(.,'BEDROOM')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.lower().split('bed')[0].strip())
        
        bathroom_count = response.xpath("//li[contains(.,'BATHROOM')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.lower().split('bath')[0].strip())
        
        rent = response.xpath("//br/following-sibling::text()[contains(.,' pcm')]").get()
        if rent:
            rent = rent.split('£')[1].split('pcm')[0].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'GBP')
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='property-gallery']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [x for x in response.xpath("//a[contains(@href,'floorplan')]/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        energy_label = response.xpath("//a[contains(@href,'EE_')]/@href").get()
        if energy_label:
            energy_label = int(energy_label.split('_')[-2].strip())
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

        pets_allowed = response.xpath("//li[contains(.,'NO PETS')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", False)
        
        parking = response.xpath("//li[contains(.,'PARKING')]").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//li[contains(.,'FURNISHED PROPERTY') or contains(.,' FURNISHED')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            furnished = response.xpath("//li[contains(.,'UNFURNISHED')]").get()
            if furnished:
                item_loader.add_value("furnished", False)

        item_loader.add_value("landlord_name", "Tully and Co")

        landlord_phone = response.xpath("//span[@class='InfinityNumber']/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())

        yield item_loader.load_item()