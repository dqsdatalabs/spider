# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader


class EllisandcoSpider(scrapy.Spider):
    name = "ellisandco"
    allowed_domains = ["www.ellisandco.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.ellisandco.co.uk/property?intent=rent&location=&radius=&type=flats_apartments&price-per=pcm&bedrooms=&include-sold=rent&sort-by=price-desc&per-page=24', 'property_type': 'apartment'},
            {'url': 'https://www.ellisandco.co.uk/property?intent=rent&location=&radius=&type=houses&price-per=pcm&bedrooms=&include-sold=rent&sort-by=price-desc&per-page=24', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )
    
    def parse(self, response, **kwargs):
        for item in response.xpath("//div[contains(@class,'item')]"):
            f_url = item.xpath(".//a/button[contains(.,'More information')]/parent::a/@href").get()
            status = item.xpath(".//p[@class='property-status'][contains(.,'Let Agreed')]").get()
            if not status:
                yield scrapy.Request(response.urljoin(f_url), callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        
        last_page = response.xpath("//a[@class='pagination-item']//text()").extract()
        for i in range(2, int(last_page[-1])+1):
            f_url = f"https://www.ellisandco.co.uk/property?p={i}&per-page=24&intent=rent&price-per=pcm&type=flats_apartments&include-sold=rent&sort-by=price-desc"
            yield scrapy.Request(url=f_url, callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        property_type = response.meta.get('property_type')
        external_link = response.url
        address = response.xpath('//h2[@class="text-secondary"]/text()').extract_first()
        if address:
            zipcode = address.split(" ")[-1]
            item_loader.add_value("zipcode", zipcode)
            city = address.split(",")[-1].split(zipcode)[0].strip()
            item_loader.add_value("city", city)

        room_count_text = response.xpath('//h2[contains(text(), "Bed")]/text()').extract_first('').strip()
        if room_count_text:
            try:
                room_count = re.findall(r'\d+', room_count_text)[0]    
            except:
                room_count = ''
        else:
            room_count = ''
        rent_string = response.xpath('//h2[contains(text(), "pcm")]/text()').extract_first('').strip().split(' | ')[-1]
        square_meters = response.xpath('//ul[@id="propList"]/li[contains(text(), "Sq")]/text()').extract_first('').split('/ ')[-1]
        description = ''.join(response.xpath('//ul[contains(@id, "propList")]/li/text()').extract())
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_value('external_id', external_link.split("/")[-1])
        item_loader.add_value('address', address)
        item_loader.add_value('description', description)
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('images', '//div[@id="animated-thumbnails"]//img/@src')
        if room_count:
            item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('square_meters', square_meters)
        
        latitude_longitude = response.xpath("//script[contains(.,'L.marker([')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("L.marker([")[1].split(",")[0]
            longitude = latitude_longitude.split("L.marker([")[1].split(",")[1].split("]")[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        furnished = response.xpath("//li[contains(.,'Furnished')]/text()[not(contains(.,'Un'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)
            
        balcony = response.xpath("//li[contains(.,'Balcon') or contains(.,'BALCON') or contains(.,'balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        from word2number import w2n
        bathroom_count = response.xpath("//li[contains(.,'BATHROOM') or contains(.,'Bathroom') or contains(.,'bathroom')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(" ")[0]
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)
            else:
                try:
                    bathroom_count = w2n.word_to_num(bathroom_count)
                    item_loader.add_value("bathroom_count", bathroom_count)
                except: pass
        
        floor_plan_images = response.xpath("//button[contains(.,'Floor plan')]/@data-src").get()
        item_loader.add_value("floor_plan_images", floor_plan_images)
                
        item_loader.add_value('landlord_name', 'Ellis & Co - Islington')
        item_loader.add_value('landlord_email', 'islington@ellisandco.co.uk')
        item_loader.add_value('landlord_phone', '020 7354 0909')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        
        yield item_loader.load_item()