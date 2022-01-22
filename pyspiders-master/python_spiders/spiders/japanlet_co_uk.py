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
    name = 'japanlet_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):
        start_urls = [
            
            {
                "url": [
                    "https://www.japanlet.co.uk/advanced-search/?filter_search_action%5B%5D=to-let&filter_search_type%5B%5D=studio&advanced_area=&no-of-bedrooms=&min-price=&max-price=",
                ],
                "property_type": "studio"
            },
            {
                "url": [
                    "https://www.japanlet.co.uk/advanced-search/?filter_search_action%5B%5D=to-let&filter_search_type%5B%5D=flat&advanced_area=&no-of-bedrooms=&min-price=&max-price=",
                    "https://www.japanlet.co.uk/advanced-search/?filter_search_action%5B%5D=to-let&filter_search_type%5B%5D=conversion&advanced_area=&no-of-bedrooms=&min-price=&max-price=",
                    "https://www.japanlet.co.uk/advanced-search/?filter_search_action%5B%5D=to-let&filter_search_type%5B%5D=apartment&advanced_area=&no-of-bedrooms=&min-price=&max-price="
                  
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.japanlet.co.uk/advanced-search/?filter_search_action%5B%5D=to-let&filter_search_type%5B%5D=maisonette&advanced_area=&no-of-bedrooms=&min-price=&max-price=",
                    "https://www.japanlet.co.uk/advanced-search/?filter_search_action%5B%5D=to-let&filter_search_type%5B%5D=house&advanced_area=&no-of-bedrooms=&min-price=&max-price="

                ],
                "property_type": "house"
            },
            
            {
                "url": [
                    "https://www.japanlet.co.uk/advanced-search/?filter_search_action%5B%5D=to-let&filter_search_type%5B%5D=room&advanced_area=&no-of-bedrooms=&min-price=&max-price=",
                    "https://www.japanlet.co.uk/advanced-search/?filter_search_action%5B%5D=to-let&filter_search_type%5B%5D=room-double&advanced_area=&no-of-bedrooms=&min-price=&max-price="
                ],
                "property_type": "room"
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
        
        for item in response.xpath("//h4/a/@href").getall():   
            yield Request(item, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//li[@class='roundright']/a/@href").get()
        if next_page:   
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Japanlet_Co_PySpider_united_kingdom")
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
            item_loader.add_value("address", title.strip())
        item_loader.add_xpath("city","//div[@class='wpb_wrapper']//h5/a[last()]/text()")
        square_meters = response.xpath("//div[strong[.='Property Size:']]/text()[normalize-space()]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0])
         
        item_loader.add_xpath("bathroom_count", "//div[strong[.='Bathrooms:']]/text()")
        item_loader.add_xpath("rent_string", "//div[strong[.='Price:']]/text()[normalize-space()]")
   
        description = " ".join(response.xpath("//div[@class='wpestate_estate_property_details_section  ']/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        external_id = response.xpath("//div[strong[.='Property Id :']]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
            
        latitude = response.xpath("//div[@id='gmap_wrapper']/@data-cur_lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        longitude = response.xpath("//div[@id='gmap_wrapper']/@data-cur_long").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
 
        parking = response.xpath("//div[strong[.='Off Street Parking:']]/text()").get()
        if parking:
            if "yes" in parking.lower():
                item_loader.add_value("parking",True)
        furnished = response.xpath("//div[strong[.='Furnishing:']]/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished",False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished",True)
        room_count = response.xpath("//div[strong[.='Bedrooms:']]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif response.meta.get('property_type') == "studio" or response.meta.get('property_type') == "room":
            item_loader.add_value("room_count", "1")
       
        energy_label = response.xpath("//div[strong[.='Energy Efficiency Band:']]/text()[normalize-space()]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())

        images = [response.urljoin(x) for x in response.xpath("//div[@class='carousel-inner']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
   
        item_loader.add_value("landlord_name", "Japan Letting")
        item_loader.add_value("landlord_phone", "020 8993 6100")
    
        yield item_loader.load_item()