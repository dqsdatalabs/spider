# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re 
from ..loaders import ListingLoader
import json
from python_spiders.helper import string_found, remove_white_spaces

class ZingatSpider(scrapy.Spider): 
    name = 'zingat' 
    allowed_domains = ['zingat']
    start_urls = ['https://www.zingat.com/']
    execution_type = 'testing' 
    country = 'turkey'
    locale ='tr'
    thousand_separator=',' 
    scale_separator='.'
    
    def start_requests(self):
        start_urls = [
            {'url': 'https://www.zingat.com/kiralik-villa', 'property_type': 'house'},
            {'url': 'https://www.zingat.com/kiralik-daire', 'property_type': 'apartment'},
            {'url': 'https://www.zingat.com/kiralik-rezidans', 'property_type': 'apartment'},
            {'url': 'https://www.zingat.com/kiralik-mustakil-ev', 'property_type': 'house'},
            {'url': 'https://www.zingat.com/kiralik-kosk-konak-yali', 'property_type': 'house'},

            
            
        ]
        #https://www.zingat.com/ege-emlaktan-mamurbabada-mustakil-havuzlu-bahceli-sezonluk-3664598i
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        page_number_index = response.xpath('//li[contains(@class, "pagination-next")]/a/@data-page').extract_first()
        page_number = int(page_number_index) + 1
        for page in range(1, page_number):
            link = "https://www.zingat.com/kiralik-villa?page={}".format(page)
            yield scrapy.Request(url=link, callback=self.get_property_links, meta={'property_type': response.meta.get('property_type')}, dont_filter=True) 

    def get_property_links(self, response, **kwargs):
        links = response.xpath('//ul[@class="zc-viewport"]/li')
        for link in links: 
            url = response.urljoin(link.xpath('./a/@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response): 
        item_loader = ListingLoader(response=response)

        holiday = "".join(response.xpath("//h1[@itemprop='name']/text()").extract())
        if "sezonluk" in holiday.lower():
            return 

        external_id = response.xpath('//strong[contains(text(), "İlan")]/following-sibling::span/text()').extract_first()
        if external_id:
            item_loader.add_value('external_id', external_id.strip())
        external_link = response.url
        if "guzellik-salon" in external_link or "sirket" in external_link or "restaurant" in external_link or "bufe" in external_link or "oto-yikama" in external_link or "dukkan" in external_link or "depo" in external_link or "cafe" in external_link or "fabrika" in external_link or "kafe" in external_link or "magaza" in external_link or "isyeri" in external_link or "fab" in external_link or "ofis" in external_link or "santral" in external_link:
            return
        address = remove_white_spaces(response.xpath('//div[@class="detail-location-path__map"]/h2/text()').extract_first())
        city = address.split(",")[-1] 
        titlee=response.xpath("//meta[@name='og:title']/@content").get()
        if titlee and "ofis" in titlee.lower():
            return 

        property_details = response.xpath('//input[@id="listingDetailInfo"]/@value').extract_first()
        data = json.loads(property_details)
        rent = str(data['listing_totalvalue']) + data['listing_currency']
        try:
            if '+' in data['listing_roomcount']: 
                room_count_value = data['listing_roomcount'].split('+')
                room_count = str(int(room_count_value[0]) + int(room_count_value[1]))
            elif 'null' in data['listing_roomcount']:
                room_count = ''  
            else:
                room_count_text = data['listing_roomcount'] 
                room_count = re.findall(r'\d+', room_count_text)[0]
        except:
            room_count = ''

        lat = response.xpath('//div[@id="details"]/@data-lat').extract_first()
        lon = response.xpath('//div[@id="details"]/@data-lon').extract_first()
        if data.get("listing_floorarea"):
            square_meters = str(data['listing_floorarea'])
        details_text = ''.join(response.xpath('//div[@class="col-md-3 attr-item"]//text()').extract())
        property_type = response.meta.get('property_type')
        floor = response.xpath('//strong[contains(text(), "Binadaki")]/following-sibling::span/text()').extract_first('').strip() 

        
        if property_type:
            item_loader.add_value('property_type', property_type)
        else: return
        
        
        item_loader.add_value('external_link', external_link)
        item_loader.add_xpath('title', '//meta[@name="og:title"]/@content')
        item_loader.add_value('address', address)
        item_loader.add_xpath('description', '//div[@class="detail-text-desktop"]/p//text()')
        item_loader.add_value('rent_string', rent)
        item_loader.add_xpath('images', '//div[@class="gallery-container"]/a[@data-image-id]/@data-lg')
        item_loader.add_value('square_meters', square_meters)
        bathroom_count = "".join(response.xpath("//strong[contains(.,'Banyo')]/following-sibling::span/text()").getall())
        if bathroom_count:

            item_loader.add_value("bathroom_count", bathroom_count.strip().replace("ve üzeri",""))

        furnished = "".join(response.xpath("//ul[contains(@class,'attribute-detail-list')]/li/strong[.='Mobilya Durumu']/following-sibling::span/text()").getall())
        if furnished:
            item_loader.add_value("furnished", True)

        item_loader.add_value('floor', floor)
        if string_found(['otopark'], details_text):
            item_loader.add_value('parking', True)
        if string_found(['balkon'], details_text):
            item_loader.add_value('balcony', True)
        if string_found(['asansör'], details_text):
            item_loader.add_value('elevator', True)
        if string_found(['terrasse', 'teras'], details_text):
            item_loader.add_value('terrace', True)
        if lat:
            item_loader.add_value('latitude', lat)    
        if lon:
            item_loader.add_value('longitude', lon)
        
        swimming_pool = response.xpath("//div[contains(@class,'attr-item') and not(contains(@class,'passive'))][contains(.,'Havuz')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        deposit = response.xpath("//strong[contains(.,'Depozito')]/following-sibling::span/text()").get()
        if deposit:
            deposit = deposit.strip().split(" ")[0].replace(".","").replace(",","")
            if deposit != "0":
                item_loader.add_value("deposit", deposit)
        
        item_loader.add_value('city', city)
        item_loader.add_value('room_count', room_count)
        item_loader.add_value('landlord_name', 'Zingat')
        item_loader.add_value('landlord_email', 'icerik@zingat.com')
        item_loader.add_value('landlord_phone', '0850 532 0 505')
        item_loader.add_value('external_source', 'Zingat_PySpider_turkey_tr')
        yield item_loader.load_item()


         