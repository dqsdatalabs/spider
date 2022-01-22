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
    name = 'arronjames_com'    
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
                "type" : "House - Mid Terrace",
                "property_type" : "house"
            },
            {
                "type" : "Flat",
                "property_type" : "apartment"
            },
            {
                "type" : "Apartment",
                "property_type" : "apartment"
            },
            {
                "type" : "Room",
                "property_type" : "room"
            },
        ]
        for url in start_urls: 

            formdata = {
                "area": "To Rent",
                "bedrooms": "",
                "minrent": "",
                "maxrent": "",
                "minprice": "",
                "maxprice": "",
                "type[]": url.get("type"),
                "vp_location": "",
                "radius": "3",
            }

            yield FormRequest(
                url="http://arronjames.com/property-search/",
                callback=self.parse,
                formdata=formdata,
                meta={'property_type': url.get('property_type'), "type": url.get("type")}
            )

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[@class='property']"):
            follow_url = response.urljoin(item.xpath(".//h3/a/@href").get())
            is_rented = item.xpath("//div[@class='property_tagline']/text()").get()
            if not 'let agreed' in is_rented.lower():
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            p_type = response.meta.get("type")
            formdata = {
                "vp_branchid": "",
                "vp_soldlet": "",
                "vp_vebraid": "",
                "vp_area": "To Rent",
                "vp_featured": "",
                "vp_bedrooms": "",
                "vp_maxbedrooms": "",
                "vp_minprice": "",
                "vp_maxprice": "",
                "vp_minrent": "",
                "vp_maxrent": "",
                "vp_type": p_type,
                "vp_status": "",
                "vp_location": "",
                "vp_radius": "3",
                "vp_pagesize": "6",
                "vp_page": str(page),
                "vp_user1": "",
                "vp_user2": "",
                "vp_orderby": "price desc",
                "vp_view": "list",
                "vp_lat": "",
                "vp_lng": "",
                "sortby": "price desc",
            }
            yield FormRequest(
                url="http://arronjames.com/property-search/",
                callback=self.parse,
                formdata=formdata,
                meta={'property_type': response.meta.get('property_type'), "type": p_type, "page":page+1}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Arronjames_PySpider_"+ self.country + "_" + self.locale)

        external_id = response.url.split('/')[-2].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)

        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        city=response.xpath("//h1/text()").get()
        if city:
            item_loader.add_value("city",city.split(",")[-1].strip())

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        description = " ".join(response.xpath("//div[@class='vp_content_section'][1]/text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        room_count = response.xpath("//p[contains(.,'Bedrooms')]/text()").get()
        if room_count:
            room_count = room_count.lower().split('bedrooms:')[-1].split('|')[0].strip()
            if room_count != '0':
                item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//p[contains(.,'Bathrooms')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.lower().split('bathrooms:')[-1].split('|')[0].strip()
            if bathroom_count != '0':
                item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = response.xpath("//span[@class='vp-price']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split('£')[-1].replace(',', '').strip())
            item_loader.add_value("currency", 'GBP')
        
        images = [x.split('url(')[-1].split(')')[0].strip() for x in response.xpath("//div[@id='slider']//div/@style").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        floor_plan_images = [x for x in response.xpath("//h2[contains(.,'Floorplan')]/following-sibling::img[1]/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        latitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('LatLng(')[-1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('LatLng(')[-1].split(',')[1].split(')')[0].strip())

        energy_label = response.xpath("//img[contains(@src,'EE_')]/@src").get()
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
        
        item_loader.add_value("landlord_phone", '0208 8337010')
        item_loader.add_value("landlord_email", 'lettings@arronjames.com')
        item_loader.add_value("landlord_name", 'Arronjames')

        yield item_loader.load_item()
