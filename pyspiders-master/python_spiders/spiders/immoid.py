# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from dateutil import parser
import js2xml
from ..loaders import ListingLoader
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date
import dateparser

def extract_city_zipcode(_address):
    zip_city = _address.split(", ")[1]
    zipcode, city = zip_city.split(" ")
    return zipcode, city

class ImmoidSpider(scrapy.Spider):
    name = 'immoid'
    allowed_domains = ['immoid']
    start_urls = ['https://www.immoid.be/']
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator='.'
    scale_separator=','
    external_source = "Immoid_PySpider_belgium_nl"

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.immoid.be/nl/te-huur?view=list&page=1&goal=1&ptype=1', 'property_type': 'house'},
            {'url': 'https://www.immoid.be/nl/te-huur?view=list&page=1&goal=1&ptype=2', 'property_type':'apartment'}    
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})
    
    def parse(self, response, **kwargs):
        links = response.xpath('//div[@id="PropertyListRegion"]/div[contains(@class, "property-list item")]//div[contains(@class, "span4 property")]')
        for link in links:
            url_confirm = link.xpath('.//a/@href').extract_first('') 
            if url_confirm:
                url = response.urljoin(url_confirm)
                yield scrapy.Request(
                    url=url,
                    callback=self.get_property_details,
                    meta={'property_type': response.meta.get('property_type')},
                    dont_filter=True
                )
        
        if response.xpath('//div[@class="pagination "]/ul/li/a[contains(@class, "next")]'):
            next_link = response.urljoin(response.xpath('//div[@class="pagination "]/ul/li/a[contains(@class, "next")]/@href').extract_first())
            yield scrapy.Request(
                url=next_link,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        external_link = response.url
        property_type = response.meta.get('property_type')
        external_id = response.xpath('//div[contains(text(), "Unieke code")]/../div[@class="value"]/text()').extract_first('')
        if not external_id:
            external_id = response.url.split("&id=")[-1].split("&")[0]
        address_value = response.xpath('//div[contains(text(), "Adres")]/../div[@class="value"]/text()').extract_first()
        address = address_value 
        if ',' not in address_value:
            address = "Locomotiefstraat 66," + ' ' + address    
        zipcode, city = extract_city_zipcode(address)
        title = response.xpath('//div[@id="PropertyRegion"]//h3/text()').extract_first()
        title = re.sub(r'[\t\n]+', '', title)

        available_date=response.xpath("//div[div[.='Beschikbaarheid']]/div[@class='value']/text()").get()
        if available_date:
            date2 =  available_date.strip()
            if "Onmiddellijk" not in date2:
                date_parsed = dateparser.parse(
                    date2, date_formats=["%m-%d-%Y"]
                )
                if date_parsed:
                    date3 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date3)

        details_text = ''.join(response.xpath('//meta[@property="og:description"]/@content').extract())
        if response.xpath('//div[contains(text(), "slaapkamers")][@class="name"]/../div[@class="value"]/text()'):
            room_count = response.xpath('//div[contains(text(), "slaapkamers")][@class="name"]/../div[@class="value"]/text()').extract_first()
        else:
            room_count = ''
        sqare_meters_v = True
        if not response.xpath('//div[contains(text(), "opp")][@class="name"]/../div[@class="value"]/text()'):
            sqare_meters_v = ''
        images = []
        image_links = response.xpath('//div[@id="LargePhoto"]//div[@class="item"]//img')
        for image_link in image_links:
            image_url = response.urljoin(image_link.xpath('./@src').extract_first())
            if image_url not in images:
                images.append(image_url)
        terrace_texts_p = response.xpath('//div[contains(text(), "Terras")][@class="name"]/../div[@class="value"]/text()').extract_first('')
        if terrace_texts_p:
            terrace_texts = re.findall(r'([\d|\.]+)', terrace_texts_p)[0]
        else:
            terrace_texts = ''
        elevator_texts_p = response.xpath('//div[contains(text(), "Lift")][@class="name"]/../div[@class="value"]/text()').extract_first('')
        if 'Ja' in elevator_texts_p:
            elevator_texts = '1' 
        elif elevator_texts_p: 
            elevator_texts = re.findall(r'([\d|\.]+)', elevator_texts_p)[0]
        else:
            elevator_texts = ''

        utilities = response.xpath("//div[div[.='Lasten huurder']]/div[@class='value']/text()").extract_first()
        if utilities:
            item_loader.add_value('utilities', utilities.strip())
        bathroom_count = response.xpath("//div[div[.='Aantal badkamers']]/div[@class='value']/text()").extract_first()
        if bathroom_count:
            item_loader.add_value('bathroom_count', bathroom_count.strip())
        else:
            item_loader.add_xpath('bathroom_count', "//div[div[.='Aantal douchekamers']]/div[@class='value']/text()")
        if room_count: 
            
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_id', external_id)
            
            item_loader.add_value('title', title)
            item_loader.add_xpath('latitude', "substring-before(substring-after(//script/text()[contains(.,'MyCustomMarker')],'MyCustomMarker(['),',')")
            item_loader.add_xpath('longitude', "substring-before(substring-after(substring-after(//script/text()[contains(.,'MyCustomMarker')],'MyCustomMarker(['),', '),']')")
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('address', address)
            item_loader.add_xpath('rent_string', '//div[@id="PropertyRegion"]//h3[contains(text(), "€")]/text()')
            item_loader.add_xpath('description', '//meta[@property="og:description"]/@content')
            item_loader.add_xpath('square_meters', '//div[contains(text(), "opp")][@class="name"]/../div[@class="value"]/text()')
            item_loader.add_value('images', images)
            parking = response.xpath("//div[div[.='Garage']]/div[@class='value']/text()").get()
            if parking:
                item_loader.add_value('parking', True)
            elif 'parking' in details_text.lower():
                item_loader.add_value('parking', True)
            if terrace_texts and float(terrace_texts) > 0:
                item_loader.add_value('elevator', True)
            if elevator_texts and float(elevator_texts) > 0:
                item_loader.add_value('elevator', True)
            if room_count:
                item_loader.add_value('room_count', str(room_count))
            item_loader.add_value('landlord_name', 'Kantoor Mechelen')
            item_loader.add_value('landlord_email', 'mechelen@immoid.be')
            item_loader.add_value('external_source', self.external_source)
            item_loader.add_value('landlord_phone', '015 / 490 900')
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('city', city)
            yield item_loader.load_item()



         