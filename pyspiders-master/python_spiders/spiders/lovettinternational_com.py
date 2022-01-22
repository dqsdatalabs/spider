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
from datetime import datetime

class MySpider(Spider):
    name = 'lovettinternational_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    url = "https://lovettinternational.com/wp-admin/admin-ajax.php"

    def start_requests(self):
        start_urls = [
            {
                "formdata" : {
                    'action': 'apf_property_search',
                    'apf_security': 'f7733bf00c',
                    'search_data[apf_market]': 'residential',
                    'search_data[apf_dept]': 'to-let',
                    'search_data[apf_location]': '',
                    'search_data[apf_minprice]': '',
                    'search_data[apf_maxprice]': '',
                    'search_data[apf_minbeds]': '0',
                    'search_data[apf_maxbeds]': '100',
                    'search_data[apf_view]': 'grid',
                    'search_data[apf_status]': '',
                    'search_data[apf_branch]': '',
                    'search_data[apf_order]': 'price_desc'
                }
            },
        ]
        for item in start_urls:
            yield FormRequest(self.url,
                            formdata=item["formdata"],
                            dont_filter=True,
                            callback=self.parse,
                            meta={"formdata": item["formdata"]})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'apf__property__border')]"):
            status = item.xpath(".//span[contains(@class,'status')]//text()[contains(.,'Let Agreed')]").get()
            if not status:
                follow_url = response.urljoin(item.xpath(".//a[contains(@class,'arrow__link')]//@href").get())
                p_type = " ".join(item.xpath(".//h5//text()").extract())
                if "Flat" in p_type.strip() or "Apartment" in p_type.strip():
                    property_type = "apartment"
                elif "House" in p_type.strip():
                    property_type = "house"
                else:
                    property_type = False
                if property_type:
                    yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
                seen = True
                
        if page == 2 or seen:
            form_data = {
                'action': 'apf_property_search',
                'apf_security': 'f7733bf00c',
                'search_data[apf_market]': 'residential',
                'search_data[apf_dept]': 'to-let',
                'search_data[apf_location]': '',
                'search_data[apf_minprice]': '',
                'search_data[apf_maxprice]': '',
                'search_data[apf_minbeds]': '0',
                'search_data[apf_maxbeds]': '100',
                'search_data[apf_view]': 'grid',
                'search_data[apf_status]': '',
                'search_data[apf_branch]': '',
                'search_data[apf_order]': 'price_desc',
                'search_data[apf_page]': str(page),
            }
            yield FormRequest("https://lovettinternational.com/wp-admin/admin-ajax.php", formdata=form_data, callback=self.parse)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Lovettinternational_PySpider_united_kingdom")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", title)
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            city = address.split(",")[-1].strip()
            item_loader.add_value("city", city)

        room_count = response.xpath("//span[contains(@class,'bed')]//parent::li/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//span[contains(@class,'bath')]//parent::li/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        item_loader.add_value("external_id", response.url.split('/')[-2].split('-')[-1])
            
        rent = response.xpath("//div[contains(@class,'price')]//span[contains(@class,'digit')]//text()").get()
        if rent:
            item_loader.add_value("rent", rent.strip().replace(",",""))
        item_loader.add_value("currency", "GBP")
        
        desc = "".join(response.xpath("//article/h2[contains(.,'About this')]/..//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        available_date = response.xpath("//ul/li[contains(.,'Available')]/text()").get()
        if available_date:
            if "Immediately" in available_date:
                available_date = datetime.now().strftime("%Y-%m-%d")
                item_loader.add_value("available_date", available_date)
            else:
                available_date = available_date.split("Available")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//div[contains(@class,'property__gallery')]//@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor = response.xpath("//ul/li[contains(.,'Floor')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("Floor")[0].strip())
        
        parking = response.xpath("//ul/li[contains(.,'Parking') or contains(.,'Garage')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//ul/li[contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//ul/li[contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        energy_label = response.xpath("//div/h2[contains(.,'EPC')]/parent::div//img/@src").get()
        if energy_label:
            energy_label = energy_label.split("Current=")[1].split("&")[0]
            item_loader.add_value("energy_label", energy_label_calculate(energy_label))
        
        latitude = response.xpath("//div//@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        
        longitude = response.xpath("//div//@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_value("landlord_name", "LOVETT ESTATE & LETTING AGENT")
        item_loader.add_value("landlord_phone", "01202 303044")
        item_loader.add_value("landlord_email", "info@lovettinternational.com")
        
        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number >= 92 and energy_number <= 100:
        energy_label = "A"
    elif energy_number > 80 and energy_number <= 91:
        energy_label = "B"
    elif energy_number > 68 and energy_number <= 80:
        energy_label = "C"
    elif energy_number > 54 and energy_number <= 68:
        energy_label = "D"
    elif energy_number > 38 and energy_number <= 54:
        energy_label = "E"
    elif energy_number > 20 and energy_number <= 38:
        energy_label = "F"
    elif energy_number <= 20:
        energy_label = "G"
    return energy_label