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
from datetime import datetime
import dateparser

class MySpider(Spider):
    name = 'alliance_london_co_uk'    
    start_urls = ['https://www.alliance-london.co.uk/?id=42015&action=view&route=search&view=list&input=united%20kingdom&jengo_property_for=2&jengo_radius=20&jengo_min_price=0&jengo_min_beds=0&jengo_max_beds=9999&jengo_max_price=99999999999&jengo_category=1&jengo_order=6&jengo_property_type=-1&pfor_complete=on&pfor_offer=on&trueSearch=U&searchType=postcode&latitude=&daterange=undefined&longitude=#total-results-wrapper']#["https://www.alliance-london.co.uk/search.ljson?channel=lettings&fragment=page-1"]
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
        "HTTPERROR_ALLOWED_CODES": [404]
    }

    # 1. FOLLOWING
    def parse(self, response):
            for item in response.xpath('//div[@class="col-md-6 marg-b-20"]/a/@href').extract():
                follow_url = response.urljoin(item)
                yield Request(follow_url, callback=self.populate_item)
            
            next_page = response.xpath('//a[@class="next-prev"]/@href').get()
            if next_page:
                p_url = response.urljoin(next_page)
                yield Request(
                p_url,
                callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "AllianceLondon_PySpider_" + self.country + "_" + self.locale)

        prop_type = response.xpath('//span[@class="description-text-right"]/strong/text()').get()
        if prop_type and 'house' in prop_type.lower():
            item_loader.add_value("property_type", 'house')
        elif prop_type and 'apartment' in prop_type.lower():
            item_loader.add_value("property_type", 'apartment')
        else:
            return
        
        let = response.xpath('//span[@class="badge-r"]/text()').get()
        if "let agreed" in let.lower() or 'withdraw' in let.lower():
            pass
        else:
            item_loader.add_value("external_link", response.url)
            title = response.xpath('/html/head/title/text()').get()
            if title:
                item_loader.add_value("title", title)
                
            address = ' '.join(response.xpath('//span[@class="details-address"]//text()').getall())
            if address:
                address = re.sub('\s{2,}', '', address).replace(',,', ' ')
                item_loader.add_value('address', address)

            room = response.xpath('//span[contains(., "Bed")]/../following-sibling::span/text()').get()
            if room:
                item_loader.add_value("room_count", room.strip())

            bathroom = response.xpath('//span[contains(., "Bath")]/following-sibling::span/text()').get()
            if bathroom:
                item_loader.add_value("bathroom_count", bathroom.strip())

            price = response.xpath("//a[@class='btn btn-third pull-right']//text()").re(r'£(\d,\d+)')
            if price:
                item_loader.add_value("rent", price[0].replace(',', ''))

            square_meters = response.xpath('//span[contains(., "Area")]/following-sibling::span/text()').get()
            if square_meters:
                square_meters = square_meters.split('sqm')[0].strip()
                if square_meters != '0.00':
                    item_loader.add_value("square_meters", square_meters)
            
            desc = ''.join(response.xpath('//p[@class="description-text"]/following-sibling::p//text()').getall())
            if desc:
                item_loader.add_value("description", desc)

            available_date= response.xpath('//span[contains(., "Date")]/following-sibling::span/text()').get()
            if available_date:
                date = available_date.strip()
                date_parsed = dateparser.parse(date, date_formats=["%d-%m-%Y"])
                date3 = date_parsed.strftime("%Y-%m-%d")

                current_date = str(datetime.now()) 
                if current_date > date3:
                    date = datetime.now().year +1
                    parsed = date3.replace("2020", str(date))
                    item_loader.add_value("available_date", parsed)

            features = "".join(response.xpath('//div[@id="features"]//text()').getall()).lower()
            if 'swimming' in features:
                item_loader.add_value("swimming_pool", True)
            else:
                if "swimming pool" in desc:
                    item_loader.add_value("swimming_pool", True)

            if "parking" in features:
                item_loader.add_value("parking", True)
            
            if "furnished" in features:
                item_loader.add_value("furnished", True)

            external_id = response.url.split('/')[-2].strip()
            if external_id:
                item_loader.add_value("external_id", external_id)
                
            images=[x for x in response.xpath('//div[@class="fotorama"]//img/@src').getall()]
            if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", str(len(images)))


            item_loader.add_value("landlord_name", "LONDON BRANCH")
            item_loader.add_value("landlord_phone", "020 7096 1588")
            item_loader.add_value("landlord_email", "info@alliance-london.co.uk")
            lat = response.xpath("//script/text()[contains(.,'latitude')]").re_first(r'"latitude":"(\d{2}\.\d+)')
            lng = response.xpath("//script/text()[contains(.,'latitude')]").re_first(r'"longitude":"(-*\d.\d+)"')
            if lat and lng:                        
                item_loader.add_value("latitude",lat)
                item_loader.add_value("longitude",lng)              

            yield item_loader.load_item()
