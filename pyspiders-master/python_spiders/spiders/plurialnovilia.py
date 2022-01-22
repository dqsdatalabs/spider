# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader
from python_spiders.helper import string_found, remove_white_spaces

def getSqureMtr(text):
    list_text = re.findall(r'\d+',text)

    if len(list_text) == 3:
        output = float(list_text[0]+"."+list_text[1])
    elif len(list_text) == 2:
        output = float(list_text[0]+"."+list_text[1])
    elif len(list_text) == 1:
        output = int(list_text[0])
    else:
        output=0

    return int(output)

class PlurialNoviliaSpider(scrapy.Spider):
    name = "plurialnovilia"
    allowed_domains = ["www.plurial-novilia.fr"]
    start_urls = (
        'http://www.www.plurial-novilia.fr/',
    )
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.plurial-novilia.fr/location/appartement/', 'property_type': 'apartment'},
            {'url': 'https://www.plurial-novilia.fr/location/maison/', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//h3[@class="hide"]/a')
        for link in links:
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        external_link = response.url
        property_type = response.meta.get('property_type')
        square_meters = str(response.xpath('//span[@itemprop="floorSize"]/text()').extract_first('').strip())
        room_count = str(response.xpath('//span[@itemprop="numberOfRooms"]/text()').extract_first('').strip())
        street = response.xpath('//span[@itemprop="streetAddress"]/text()').extract_first('').strip()
        zipcode = response.xpath('//span[@itemprop="postalCode"]/text()').extract_first('').strip()
        city = response.xpath('//span[@itemprop="addressLocality"]/text()').extract_first('').strip()
        address = street + ' ' + zipcode + ' ' + city   
        energy_label = response.xpath('//li[contains(@class, "ico_product_dpe_c")]/strong/text()').extract_first('').strip()
        utilities = response.xpath('//p[contains(text(), "Dont charges")]/text()').extract_first('').strip()
        deposit_text = response.xpath('//p[contains(text(), "Dépôt")]/text()').extract_first('').strip() 
        laborn_phone = response.xpath('//span[contains(text(), "Afficher le numéro")]/../@href').extract_first('').strip()

        floor = "".join(response.xpath("substring-before(//div[contains(@class,'product_tools')]/ul/li[contains(.,'étage')]/text(),' ')").extract())
        if floor:
            item_loader.add_value('floor', floor.strip())

        if room_count: 
            
            if property_type:
                item_loader.add_value('property_type', property_type)
            item_loader.add_xpath('external_id', '//span[@itemprop="productID"]/text()')
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('address', address)
            item_loader.add_xpath('latitude', "//p[@class='hide']/span[@class='lat_push']/text()")
            item_loader.add_xpath('longitude', "//p[@class='hide']/span[@class='lng_push']/text()")
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            if utilities:
                item_loader.add_value('utilities', getSqureMtr(utilities.split(':')[1])) 
            if deposit_text:
                item_loader.add_value('deposit', getSqureMtr(deposit_text.split(':')[1])) 
            if energy_label:
                item_loader.add_value('energy_label', str(energy_label)) 

            rent = "".join(response.xpath("//span[@class='loyer_price']/span/text()").extract())
            if rent:
                price = rent.split(" ")[0].strip()
                item_loader.add_value('rent', int(float(price)))
                item_loader.add_value('currency', 'EUR')

            meters = "".join(response.xpath("//span[@itemprop='floorSize']/text()").extract())
            if meters:
                item_loader.add_value('square_meters', int(float(meters)))

            item_loader.add_xpath('title', '//title/text()')
            item_loader.add_xpath('description', '//meta[@property="og:description"]/@content')
            item_loader.add_xpath('images', '//img[@class="img-responsive"]/@src')

            item_loader.add_value('room_count', room_count)
            item_loader.add_value('landlord_name', 'Plurial Novilia')
            item_loader.add_value('landlord_email', 'plurial-novilia.dpo@plurial.fr')
            item_loader.add_value('landlord_phone', laborn_phone.split(':')[1])
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            yield item_loader.load_item()