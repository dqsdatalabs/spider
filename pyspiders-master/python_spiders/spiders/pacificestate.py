# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader

class PacificestateSpider(scrapy.Spider):
    name = "pacificestate"
    allowed_domains = ["pacificestate.co.uk"]
    start_urls = (
        'http://www.pacificestate.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    custom_settings = {
        "PROXY_ON" :"True"
    }
    
    def start_requests(self):
        start_urls = "https://www.pacificestate.co.uk/notices?c=44|46|48&p=1&q=&min_price=&max_price=&filter_attribute[numeric][2][min]=&filter_attribute[numeric][2][max]="
        yield scrapy.Request( url=start_urls, callback=self.parse, dont_filter=True )


    def parse(self, response, **kwargs):
        page_numbers = response.xpath('//span[@id="cout"]/text()').extract_first().split(' - ')[-1].split(' of ')[0]
        total_page = int(page_numbers) + 1 
        for page in range(1, total_page):
            link = response.url.replace('p=1', 'p={}'.format(page))
            yield scrapy.Request( url=link, callback=self.get_detail_urls, dont_filter=True )

    def get_detail_urls(self, response):
        links = response.xpath('//a[@class="feature_img"]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True)
    
    def get_property_details(self, response):
        # parse details of the propery
        property_site_type = response.xpath('//div[contains(text(), "Property Type")]/following-sibling::div/text()').extract_first('')
        if 'house' in property_site_type.lower():
            property_type = 'house'
        elif 'apartment' in property_site_type.lower():
            property_type = 'apartment'
        elif 'flat' in property_site_type.lower():
            property_type = 'apartment'
        else:
            property_type = '' 
        if property_type: 
            item_loader = ListingLoader(response=response)
            external_link = response.url
            external_id = response.xpath('//meta[@property="fb:app_id"]/@content').extract_first().strip()
            address = response.xpath('//div[@class="as_proeprty_map_location"]/text()').extract_first()
            zipcode = address.split(', ')[-1].strip()
            if zipcode:
                if zipcode.count(" ") >1: zipcode = ""
                elif "&" in zipcode: zipcode = ""
                
            city = address.replace(zipcode, '') 
            room_count = response.xpath('//div[contains(@class, "property_feature")]//div[contains(text(), "Bedroom")]/preceding-sibling::div/text()').extract_first('').strip()
            bathrooms = response.xpath('//div[contains(text(), "Baths")]/following-sibling::div/text()').extract_first('').strip() 
            lat = response.xpath('//input[@id="board_latitude"]/@value').extract_first()
            lon = response.xpath('//input[@id="board_longitude"]/@value').extract_first()
            rent = response.xpath('//div[@id="price_value"]/text()').extract_first('').strip()
            if rent:
                if "pw" in rent:
                    rent = rent.split(" ")[0].replace("£","")
                    item_loader.add_value("rent_string", str(int(float(rent))*4)+"£")
                else:
                    item_loader.add_value('rent_string', rent.split(" ")[0].replace(",",""))
                
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_link', external_link)
            item_loader.add_xpath('title', "//div[contains(@class,'as_proeprty_map_title')]/text()")
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            if zipcode:
                item_loader.add_value('zipcode', zipcode)
            item_loader.add_xpath('description', '//div[@id="full_notice_description"]//text()')
            item_loader.add_xpath('images', '//div[contains(@class, "slider")]//img/@src')
            if room_count:
                item_loader.add_value('room_count', str(room_count))
            item_loader.add_value('latitude', str(lat))
            item_loader.add_value('longitude', str(lon))
            if bathrooms: 
                item_loader.add_value('bathroom_count', str(bathrooms))
            
            import dateparser
            available_date = response.xpath("//div[contains(@class,'col col_100')]/div[contains(.,'Available')]/following-sibling::div/text()").get()
            if available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            
            item_loader.add_value('landlord_name', 'Pacific Estates')
            item_loader.add_value('landlord_email', 'office@pacificestate.co.uk')
            item_loader.add_value('landlord_phone', '020 7177 7888')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            yield item_loader.load_item()