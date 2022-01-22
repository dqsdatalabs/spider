# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date, remove_white_spaces, remove_unicode_char
import re
# from datetime import datetime

class AllanFullerCoUk(scrapy.Spider):

    name = "fieldpalmer_com"
    allowed_domains = ["fieldpalmer.com"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    position = 0
    api_url = "https://fieldpalmer.com/properties"
    params = {
        '_method': 'POST',
        'data[Search][sale]': '2',
        'data[Search][max_price]': '0',
        'data[Search][min_beds]': '',
        'data[Search][location]': '',
        'data[Search][prop_sub]': '',
        'data[Search][latitude]': ''
    }

    def start_requests(self):
        start_urls = [
            {
                "type" : "f",
                "property_type" : "apartment",
            },
            {
                "type" : "h",
                "property_type" : "house"
            },
            {
                "type" : "b",
                "property_type" : "house"
            },
        ]
        
        for url in start_urls:
            self.params["data[Search][prop_sub]"]= url.get("type")
            yield scrapy.FormRequest(
                url=self.api_url,
                callback=self.parse,
                method='POST',
                dont_filter=True,
                formdata=self.params,
                meta={'property_type': url.get("property_type")}
            )

    def parse(self, response, **kwargs):
        listings = response.xpath("//div[contains(@class,'property')]//div[contains(@class,'image')]//a")
        for property_item in listings:            
            url = property_item.xpath('@href').extract_first()
            yield scrapy.Request(
                url = response.urljoin(url),
                callback = self.get_property_details,
                meta = {'request_url' : response.urljoin(url)})

    def get_property_details(self, response):

        external_link = response.meta.get('request_url')

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "FieldPalmer_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_link.split('/')[-2].split('_')[-1])

        title = response.xpath('//head/title/text()').extract_first()
        item_loader.add_value('title',title)

        address = response.xpath('//h2[@class="address"]/text()').extract_first()
        if address:
            item_loader.add_value('address',address)
            item_loader.add_value('city',address.split(', ')[-1])
        
        room_rent = response.xpath('//h2[@class="address"]/../h2[2]/text()').extract_first()
        if room_rent:
            room = extract_number_only(room_rent.split(' | ')[0])
            item_loader.add_value('room_count',room)
            rent = extract_number_only(room_rent.split(' | ')[-1],thousand_separator=',',scale_separator='.')
            item_loader.add_value('rent', rent)
            item_loader.add_value("currency", "GBP")
        
        item_loader.add_value("landlord_name", "FIELD PALMER ESTATE AGENTS")
        item_loader.add_xpath('landlord_phone','//h2[@class="office"]/span/text()')
        item_loader.add_value('landlord_email','lettings@fieldpalmer.com')
        
        map = response.xpath('//a[@class="mobile"]/@href').extract_first()
        if map:
            map = map.split('q=')[-1]
            item_loader.add_value('latitude',map.split(',')[0])
            item_loader.add_value('longitude',map.split(',')[-1])

        description = " ".join(response.xpath("//div[contains(@class,'shrink')]//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        deposit = response.xpath("//div[contains(@class,'shrink')]//text()[contains(.,'DEPOSIT')]").get()
        if deposit:
            deposit = deposit.split("£")[1].split(".")[0].replace(",","")
            item_loader.add_value("deposit", deposit)

        item_loader.add_xpath('images','//a[@class="photo_swipe"]/img/@src | //div[contains(@id,"gallery_div")]//@src')

        apartment_types = ["lejlighed", "appartement", "apartment", "piso", "flat", "atico",
                           "penthouse", "duplex", "dakappartement", "triplex"]
        house_types = ['hus', 'chalet', 'bungalow', 'maison', 'house', 'home', 'villa', 'huis', 'cottage', 'student property', 'masionette']
        studio_types = ["studio"]
        
        for i in studio_types:
            if i in description.lower():
                item_loader.add_value('property_type','studio')
        for i in house_types:
            if i in description.lower():
                item_loader.add_value('property_type','house')
        for i in apartment_types:
            if i in description.lower():
                item_loader.add_value('property_type','apartment')

        if not item_loader.get_collected_values('property_type'): return
        
        self.position+=1
        item_loader.add_value('position',self.position)
        status = response.xpath("//div[contains(@class,'status')]//text()[contains(.,'Let Agreed')]").get()
        if status:
            return

        yield item_loader.load_item()