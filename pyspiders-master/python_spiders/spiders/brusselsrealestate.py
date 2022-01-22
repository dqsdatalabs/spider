# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader

def extract_city_zipcode(_address):
    zipcode_city = _address.split('. ')[1] 
    zipcode, city = zipcode_city.split(" ")
    return zipcode, city
class BrusselsrealestateSpider(scrapy.Spider):
    name = "brusselsrealestate"
    allowed_domains = ["brusselsrealestate"]
    start_urls = (
        'https://brusselsrealestate.eu/fr/a-louer/',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator='.'
    scale_separator=','

    def start_requests(self):
        
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        links = response.xpath('//div[@class="msimmo-property-list"]/a')
        for link in links:
            url = link.xpath('./@href').extract_first()
            property_ts = link.xpath('.//div[@class="msimmo-property-list-item-info-type"]/text()').extract_first().strip().replace(' ', '')
            yield scrapy.Request(url, callback=self.parse_get_details, meta={'property_type': property_ts}, dont_filter=True )
    def parse_get_details(self, response): 

        rented = response.xpath("//div[@class='msimmo-property-list-item-status']/text()").extract_first()       
        if "Loué" in rented:return

        external_id = response.url.split('/')[-2]
        title = ' '.join(response.xpath('//div[@class="msimmo-slide-caption-text"]//text()').extract()).strip()
        address = response.xpath('//div[@class="contact-address"]/text()').extract_first().strip()
        zipcode, city = extract_city_zipcode(address)
        if 'appartement' in response.meta.get('property_type').lower():
            property_type = 'apartment'
        elif 'studio' in response.meta.get('property_type').lower():
            property_type = 'studio'
        else:
            property_type = 'house'
        try:
            price = re.search(r'Prix:\s(.*?)<br />', response.text, re.S|re.M|re.I).group(1)
            price = re.sub(r'[\s]+', '', price)
        except:
            price = ''
        # try:
        #     room_count = re.search(r'Nombre de chambre\(s\):\s(.*?)<br />', response.text, re.S|re.M|re.I).group(1)
        # except:
        #     room_count = ''
        try:
            address = re.search(r'Adresse:\s(.*?)<br />', response.text, re.S|re.M|re.I).group(1)
        except:
            address = ''
        try:
            square_meters = re.search(r'Superficie habitable:\s(.*?)<br />', response.text, re.S|re.M|re.I).group(1)
        except:
            square_meters = ''
        item_loader = ListingLoader(response=response)

        utilities = "".join(response.xpath("substring-after(//div[@class='msimmo-property-details-charges']/text()[contains(.,'Charges')],': ')").extract())
        if utilities:
            uti = utilities.split(" ")[0]
            if uti !="0":
                item_loader.add_value('utilities',uti.strip() )

        room_count = "".join(response.xpath("substring-after(//div[@class='msimmo-property-details-general']/text()[contains(.,'chambre')],': ')").extract())
        if room_count:
            
            if room_count.strip() != "0":
                item_loader.add_value('room_count', room_count.strip())

        bathroom_count = "".join(response.xpath("substring-after(//div[@class='msimmo-property-details-general']/text()[contains(.,'Nombre de salle(s) de bain')],': ')").extract())
        if bathroom_count:
            if bathroom_count !="0":
                item_loader.add_value('bathroom_count', bathroom_count)
        item_loader.add_value('external_id', external_id)
        if property_type:
            item_loader.add_value('property_type', property_type)

        furnished = "".join(response.xpath("substring-after(//div[@class='msimmo-property-details-general']/text()[contains(.,'Meublé')],': ')").extract())
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value('furnished', True)
            else:
                item_loader.add_value('furnished', False)

        item_loader.add_value('title', title)
        item_loader.add_value('address', address)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_xpath('title', "//title/text()")
        item_loader.add_value('city', city)
        item_loader.add_xpath('description', '//div[@class="msimmo-property-details-description"]/span//text()')
        item_loader.add_value('rent_string', price)
        item_loader.add_xpath('images', '//div[@class="swiper-slide"]/img/@src')
        item_loader.add_value('square_meters', square_meters)
        item_loader.add_value('landlord_name', 'Brussels')
        item_loader.add_value('landlord_email', 'info@brusselsrealestate.eu')
        item_loader.add_value('landlord_phone', '+32 (0)2 534 24 52')
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item()
