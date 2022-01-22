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
    name = 'agentandhomes_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agentandhomes.com/?id=26251&action=view&route=search&view=list&input=W11&jengo_property_for=2&jengo_property_type=8&jengo_min_beds=0&jengo_max_beds=9999&jengo_radius=5&jengo_category=1&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=2&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper",
                    "https://www.agentandhomes.com/?id=26251&action=view&route=search&view=list&input=W11&jengo_property_for=2&jengo_property_type=13&jengo_min_beds=0&jengo_max_beds=9999&jengo_radius=5&jengo_category=1&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=2&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.agentandhomes.com/?id=26251&action=view&route=search&view=list&input=W11&jengo_property_for=2&jengo_property_type=11&jengo_min_beds=0&jengo_max_beds=9999&jengo_radius=5&jengo_category=1&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=2&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper",
                ],
                "property_type" : "house"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//section[contains(@id,'result')]"):
            follow_url = response.urljoin(item.xpath(".//a[.='View Details']/@href").get())
            room_count = item.xpath(".//i[contains(@class,'bed')]/../text()").get()
            bathroom_count = item.xpath(".//i[contains(@class,'bath')]/../text()").get()
            square_meters = item.xpath(".//i[contains(@class,'square-o')]/../text()[1]").get()
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'), 'room_count': room_count, 'bathroom_count': bathroom_count, 'square_meters': square_meters})
        
        next_page = response.xpath("//a[@class='next-prev']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        short_let = ''.join(response.xpath("//span[@class='details-type']/text()").getall())
        if 'short let' in short_let.lower():
            return

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Agentandhomes_PySpider_united_kingdom")

        external_id = response.url.split('property/')[1].split('/')[0]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(',')[-1].strip())
            item_loader.add_value("city", address.split(',')[-2].strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//h5[contains(.,'Description')]/../p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))
        
        square_meters = response.meta.get('square_meters')
        if square_meters:
            item_loader.add_value("square_meters", str(int(float(square_meters.strip()))))

        room_count = response.meta.get('room_count')
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.meta.get('bathroom_count')
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//div[@class='container-slider']//h2/text()").get()
        if rent:
            if "PW" in rent.upper():
                rent = "".join(filter(str.isnumeric, rent.split('.')[0]))
                item_loader.add_value("rent", str(int(rent) * 4))
            else:
                rent = "".join(filter(str.isnumeric, rent.split('.')[0]))
                item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'GBP')
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='gallery']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor = response.xpath("//div[@id='features']/text()[contains(.,'Floor') or contains(.,'floor')]").get()
        if floor:
            floor = "".join(filter(str.isnumeric, floor.lower().split('floor')[0].split(',')[-1].strip()))
            if floor:
                item_loader.add_value("floor", floor)
        
        parking = response.xpath("//div[@id='features']/text()[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[@id='features']/text()[contains(.,'Balcony') or contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//div[@id='features']/text()[contains(.,'Unfurnished') or contains(.,'unfurnished')]").get()
        if furnished:
            item_loader.add_value("furnished", False)
        else:
            furnished = response.xpath("//div[@id='features']/text()[contains(.,'Furnished') or contains(.,'furnished')]").get()
            if furnished:
                item_loader.add_value("furnished", True)

        elevator = response.xpath("//div[@id='features']/text()[contains(.,'Lift') or contains(.,'lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//div[@id='features']/text()[contains(.,'Terrace') or contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        swimming_pool = response.xpath("//div[@id='features']/text()[contains(.,'Swimming Pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        script_map = response.xpath("//script[@type='text/javascript']/text()[contains(.,'prop_lat =')]").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("prop_lat =")[1].split(";")[0].strip())
            item_loader.add_value("longitude", script_map.split("prop_lng =")[1].split(";")[0].strip())

        item_loader.add_value("landlord_name", "Agent & Homes")
        item_loader.add_value("landlord_phone", "020 3598 0808")
        item_loader.add_value("landlord_email", "info@agentandhomes.com")
        
        yield item_loader.load_item()
