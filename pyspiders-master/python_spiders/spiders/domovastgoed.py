# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import js2xml
import re
from ..loaders import ListingLoader
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date
from scrapy import Request,FormRequest

def extract_city_zipcode(_address):
    zip_city = _address.split(", ")[1]
    zipcode, city = zip_city.split(" ")
    return zipcode, city

class DomovastgoedSpider(scrapy.Spider):
    name = 'domovastgoed'
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    external_source='Domovastgoed_PySpider_belgium_nl'
    thousand_separator=','
    scale_separator='.'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.domovastgoed.be/nl/te-huur/appartementen", "property_type": "apartment"},
	        {"url": "https://www.domovastgoed.be/nl/te-huur/woningen", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//section[@id='properties']/ul/li/article/a[not(contains(@class,'property__sold'))]"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(
                url=follow_url,
                callback=self.populate_item,
                meta={'property_type': response.meta.get('property_type')},
            )
        
        next_page = response.xpath("//section[@id='pagination']//li/a/text()").getall()
        if next_page:
            for i in range(2,int(next_page[-1])+1):
                f_url = f"https://www.domovastgoed.be/nl/te-huur/appartementen/pagina-{i}"
                yield Request(
                        url=f_url,
                        callback=self.parse,
                        meta={'property_type': response.meta.get('property_type')},
                    )
                
    def populate_item(self, response):
        
        external_link = response.url
        external_id = response.xpath('//span[contains(text(), "Ref")]/text()').extract_first().replace('Ref.:', '')
        property_type = response.meta.get('property_type')
        address = ''.join(response.xpath('//div[@class="address"]//text()').extract())
        address = re.sub(r'[\n\t]+', '', address)
        zipcode, city = extract_city_zipcode(address)
        details_text = ''.join(response.xpath('//meta[@name="og:description"]/@content').extract())
        images = []
        image_links = response.xpath('//section[@id="property__media"]/ul/li//img')
        for image_link in image_links:
            image_url = response.urljoin(image_link.xpath('./@src').extract_first())
            if image_url not in images:
                images.append(image_url)
        elevator_text = response.xpath('//dt[contains(text(), "Lift")]/following-sibling::dd/text()').extract_first('')
        parking_xpath = response.xpath('//i[contains(text(), "parking_spots")]')
        landlord_name = response.xpath('//div[@class="person__information"]/strong/text()').extract_first('')
        if not landlord_name:
            landlord_name = "Domo Vastgoed"
        landlord_email = response.xpath('//div[@class="person__information"]/a/@href').extract_first('').replace('mailto:', '')
        if not landlord_email:
            landlord_email = "info@domovastgoed.be"
        item_loader = ListingLoader(response=response)
            
        bathroom_count = response.xpath("//i[contains(@class,'bathroom')]/following-sibling::text()").get()
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value('property_type', property_type)

        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('address', address)
        rent = response.xpath('//section[@id="property__title"]/div[@class="price"]/text()[contains(.,"€")]').get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[1].replace('.', '').strip())
        item_loader.add_value("currency", "EUR")

        desc = " ".join(response.xpath("//meta[@name='og:description']/@content").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        item_loader.add_xpath('square_meters', '//i[contains(text(), "surface_livable")]/../text()')
        item_loader.add_xpath('floor', '//dt[contains(text(), "verdiepingen")]/following-sibling::dd/text()')
        item_loader.add_value('images', images)
        if elevator_text and 'Ja' in elevator_text:
            item_loader.add_value('elevator', True)
        if parking_xpath:
            item_loader.add_value('parking', True)
        if 'vaatwas' in details_text.lower():
            item_loader.add_value('dishwasher', True)
        if 'terras' in details_text.lower():
            item_loader.add_value('terrace', True)
        item_loader.add_xpath('room_count', '//i[contains(text(),"Slaapkamers")]/../text()')
        item_loader.add_value('landlord_name', landlord_name)
        item_loader.add_value('landlord_email', landlord_email)
        item_loader.add_value('landlord_phone', '+32 11 96 49 96')
        item_loader.add_value('external_source', 'Domovastgoed_PySpider_belgium_nl')
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('city', city)
        
        import dateparser
        available_date = response.xpath("//div/dt[contains(.,'Beschikbaarheid')]/following-sibling::dd/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
                
        utilities = response.xpath("//div/dt[contains(.,'kosten')]/following-sibling::dd/text()[contains(.,'€')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(",")[0].split("€")[1].strip())

        latitude = response.xpath("//script[contains(.,'Coordinate')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split("Coordinate(")[1].split(",")[0].strip())
            item_loader.add_value("longitude", latitude.split("Coordinate(")[1].split(",")[1].split(")")[0].strip())
        
        yield item_loader.load_item()



             