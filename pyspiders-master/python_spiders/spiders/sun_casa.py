# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader
import dateparser

class SunCasaSpider(scrapy.Spider):
    name = "sun_casa"
    allowed_domains = ["sun-casa.co.uk"]
    start_urls = (
        'http://www.sun-casa.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    external_source = 'Sun_casa_PySpider_united_kingdom_en'
    def start_requests(self):
        start_urls = [
            {'url': 'http://sun-casa.co.uk/properties/to-rent/?f=1&type=apartment', 'property_type': 'apartment'},
            {'url': 'http://sun-casa.co.uk/properties/to-rent/?f=1&type=flat', 'property_type': 'apartment'},
            {'url': 'http://sun-casa.co.uk/properties/to-rent/?f=1&type=house', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[@class="pagination"]/a')
        if links: 
            for link in links: 
                url = response.urljoin(link.xpath('./@href').extract_first())
                yield scrapy.Request(url=url, callback=self.get_details_urls, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        else:
            yield scrapy.Request(url=response.url, callback=self.get_details_urls, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    def get_details_urls(self, response):
        links = response.xpath('//div[@class="detail-card--horiz"]')
        for link in links: 
            if 'let' not in ''.join(link.xpath('.//div[@class="detail-card__img"]//text()').extract()).lower():
                url = response.urljoin(link.xpath('./a/@href').extract_first())
                yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        # parse details of the pro
        property_type = response.meta.get('property_type')
        external_link = response.url
        address_text = response.xpath('//h1[@class="h3"]/text()').extract_first()
        address = address_text.replace(address_text.split(', ')[0] + ', ', '')
        room_count = response.xpath('//h2[contains(text(), "Property details")]/following-sibling::ul/li[1]/span/text()').extract_first('').strip()
        bathrooms = response.xpath('//h2[contains(text(), "Property details")]/following-sibling::ul/li[2]/span/text()').extract_first('').strip() 
        rent_string = "".join(response.xpath('//h3[contains(@class, "primary property__price")]//text()').getall())
        if rent_string:
            if "week" in rent_string:
                rent = response.xpath('//h3[contains(@class, "primary property__price")]/text()').get()
                if rent:
                    rent = rent.split("£")[1].strip().replace(",","")
                    rent = int(rent)*4
            else:
                rent = response.xpath('//h3[contains(@class, "primary property__price")]/text()').get()
                if rent:
                    rent = rent.split("£")[1].strip().replace(",","")
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value('property_type', property_type)
        item_loader.add_xpath('title', "//title/text()")
        item_loader.add_value('external_link', external_link)
        external_id = response.xpath("//link[contains(@rel,'shortlink')]//@href").get()
        if external_id:
            external_id = external_id.split("p=")[-1]
            item_loader.add_value('external_id', external_id)
        if address:
            item_loader.add_value('address', address)
            item_loader.add_value('city', address.strip().split(" ")[-2])
            item_loader.add_value('zipcode', address.strip().split(" ")[-1])         

        item_loader.add_xpath('description', '//div[@class="p-t-2"]/p//text()')
        item_loader.add_value('rent', rent)
        item_loader.add_value("currency", "GBP")
        item_loader.add_xpath('images', '//div[@class="gallery__thumbnails"]//a/@href')
        # details = response.xpath('//ul[@class="dp-features-list ui-list-icons"]//li[@class="dp-features-list__item"]')
        available_date=response.xpath("//ul[@class='dp-features-list ui-list-icons']/li[contains(.,'Available ')]/span/text()").get()
        if available_date:
            date2 =  available_date.split("from")[1].strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            if date_parsed:
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)

        furnished = "".join(response.xpath("//ul[@class='dp-features-list ui-list-icons']/li[contains(.,'Furnished')]/span/text()").extract())
        if furnished:
            item_loader.add_value("furnished", True)

        if room_count:
            item_loader.add_value('room_count', str(room_count))
        if bathrooms: 
            item_loader.add_value('bathroom_count', str(bathrooms))
        item_loader.add_value('landlord_name', 'Sun Casa Properties Ltd')
        item_loader.add_value('landlord_email', 'info@sun-casa.co.uk')
        item_loader.add_value('landlord_phone', '01143 270 740')
        
        yield item_loader.load_item() 
