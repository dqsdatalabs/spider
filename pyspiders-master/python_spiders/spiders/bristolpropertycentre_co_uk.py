# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


import re
import scrapy
from scrapy import Selector
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date
import js2xml
import lxml.etree
from datetime import date,datetime


class BristolpropertycentreSpider(scrapy.Spider):
    name = "bristolpropertycentre_co_uk"
    allowed_domains = ["www.bristolpropertycentre.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    data = {'filter_cat': '2',
            'tx_placename': '',
            'filter_rad': '',
            'eapow-qsmod-types': '5',
            'selectItemeapow-qsmod-types': '5',
            'filter_keyword': '',
            'filter_beds': '',
            'filter_price_low': '',
            'filter_price_high': '',
            'commit': '',
            'filter_lat': '0',
            'filter_lon': '0',
            'filter_location': '[object Object]',
            'filter_types': '5'}
    start_url = 'https://www.bristolpropertycentre.co.uk/properties?eapowquicksearch=1&start=0'

    def start_requests(self):
        property_types = [{
            'property_type': 'apartment',
            'property_number': '5'},
            {
            'property_type': 'apartment',
            'property_number': '6'},
            {
            'property_type': 'apartment',
            'property_number': '3'},
            {
            'property_type': 'apartment',
            'property_number': '7'},
            {
            'property_type': 'apartment',
            'property_number': '8'},
            {
            'property_type': 'apartment',
            'property_number': '9'},
        ]
        for item in property_types:
            self.data['eapow-qsmod-types'] = item['property_number']
            self.data['selectItemeapow-qsmod-types'] = item['property_number']
            self.data['filter_types'] = item['property_number']
            yield scrapy.FormRequest(url=self.start_url,
                                     dont_filter=True,
                                     formdata=self.data,
                                     callback=self.parse,
                                     meta={'formdata': self.data,
                                           'property_type': item['property_type']
                                           })

    def parse(self, response, **kwargs):
        lettings = response.xpath('.//div[@id="smallProps"]/div')
        for letting in lettings:
            property_url = letting.xpath('.//p/a/@href').extract_first()
            if property_url:
                property_url = response.urljoin(letting.xpath('.//p/a/@href').extract_first())
                room_count = extract_number_only(letting.xpath('.//img[@alt="bedrooms"]/following-sibling::span/text()').extract_first())
                bathroom_count = extract_number_only(letting.xpath('.//img[@alt="bathrooms"]/following-sibling::span/text()').extract_first())
                yield scrapy.Request(url=property_url,
                                     callback=self.get_property_detials,
                                     meta={'request_url': property_url,
                                           'property_type': response.meta.get('property_type'),
                                           'room_count': room_count,
                                           'bathroom_count': bathroom_count,
                                           })

        next_page_url = response.xpath('.//a[@title="Next"]/@href').extract_first()
        if next_page_url:
            next_page_url = response.urljoin(next_page_url)
            yield scrapy.Request(url=next_page_url,
                                 dont_filter=True,
                                 # formdata=response.meta.get('formdata'),
                                 callback=self.parse,
                                 meta={'formdata': response.meta.get('formdata'),
                                       'property_type': response.meta.get('property_type')
                                       })

    def get_property_detials(self, response):
        item_loader = ListingLoader(response=response)

        if response.xpath("//img[contains(@alt,'Let STC')]").get(): return

        item_loader.add_value("external_link", response.meta.get('request_url'))
        item_loader.add_value("external_id", response.meta.get('request_url').split('/')[-1].split('-')[0])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("room_count", response.meta.get('room_count'))
        item_loader.add_value("bathroom_count", response.meta.get('bathroom_count'))
        item_loader.add_xpath('title', './/h1/text()')
        item_loader.add_xpath('rent_string', './/small[contains(@class,"price")]/text()')
        item_loader.add_xpath('images', './/div[@id="slider"]//ul[@class="slides"]//img/@src')
        item_loader.add_xpath('description', './/p[contains(text(),"|")]/text()')
        if not item_loader.get_output_value('description'):
            item_loader.add_xpath('description', '//div[@class="span12"]/p//text()')

        # address = item_loader.get_output_value('title')
        # item_loader.add_value('city', address.split(', ')[-1])

        item_loader.add_xpath('address','.//address//text()')
        if item_loader.get_output_value('address'):
            item_loader.add_value('city', item_loader.get_output_value('address').split(' ')[-3])
            item_loader.add_value('zipcode', ' '.join(item_loader.get_output_value('address').split(' ')[-2:]))

        javascript = response.xpath('.//script[contains(text(),"lat") and contains(text(),"lon")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)

            latitude = selector.xpath('.//property[@name="lat"]/string/text()').extract_first()
            longitude = selector.xpath('.//property[@name="lon"]/string/text()').extract_first()

            if latitude and longitude:
                item_loader.add_value('latitude', latitude)
                item_loader.add_value('longitude', longitude)
                """
                geolocator = Nominatim(user_agent=random_user_agent())
                location = geolocator.reverse(latitude+','+longitude)
                item_loader.add_value('address', location.address)

                if 'postcode' in location.raw['address']:
                    item_loader.add_value('zipcode', location.raw['address']['postcode'])
                """

        # https://www.bristolpropertycentre.co.uk/properties/property/6717211-braggs-lane-st-philips-bristol
        furnished = response.xpath('.//ul[@id="starItem"]/li[contains(text(),"Furnished")]/text()').extract_first()
        if furnished:
            if 'not' not in furnished.lower():
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)

        # https://www.bristolpropertycentre.co.uk/properties/property/7669938-lodore-road-bristol
        unfurnished = response.xpath('.//ul[@id="starItem"]/li[contains(text(),"Unfurnished")]/text()').extract_first()
        if unfurnished:
            item_loader.add_value("furnished", False)

        # https://www.bristolpropertycentre.co.uk/properties/property/7669938-lodore-road-bristol
        parking = response.xpath('.//ul[@id="starItem"]/li[contains(text(),"Garage") or contains(text(),"Parking")]/text()').extract_first()
        if parking:
            if 'not' not in parking.lower():
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)

        # https://www.bristolpropertycentre.co.uk/properties/property/7669938-lodore-road-bristol
        dishwasher = response.xpath('.//ul[@id="starItem"]/li[contains(text(),"dishwasher") or contains(text(),"Dishwasher")]/text()').extract_first()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        # https://www.bristolpropertycentre.co.uk/properties/property/7669938-lodore-road-bristol
        available_date = response.xpath('.//ul[@id="starItem"]/li[contains(text(),"Available")]/text()').extract_first()
        if available_date:
            available_date = re.findall(r'\d{1,2}(?:st|nd|rd|th) \w+',available_date)
            if available_date:
                available_date = available_date[0].replace('st','').replace('nd','').replace('rd','').replace('th','').replace('Augu','August')
                available_date = datetime.strptime(available_date, "%d %B")
                today = datetime.today()
                available_date = available_date.replace(year=today.year)
                if available_date<today:
                    available_date = available_date.replace(year=today.year+1)
                item_loader.add_value('available_date',available_date.strftime("%Y-%m-%d"))

        terrace = response.xpath('.//ul[@id="starItem"]/li[contains(text(),"terrace") or contains(text(),"Terrace")]/text()').extract_first()
        if terrace:
            item_loader.add_value("terrace", True)

        balcony = response.xpath('.//ul[@id="starItem"]/li[contains(text(),"balcony") or contains(text(),"Balcony")]/text()').extract_first()
        if balcony:
            item_loader.add_value("balcony", True)

        washing_machine = response.xpath('.//ul[@id="starItem"]/li[contains(text(),"Washing machine") or contains(text(),"washing machine")]/text()').extract_first()
        if washing_machine:
            item_loader.add_value("washing_machine", True)


        item_loader.add_xpath('landlord_name', './/a[contains(text(),"Bristol Property Centre")]/..//b/text()')
        item_loader.add_value("landlord_email", "info@bristolpropertycentre.co.uk")
        landlord_phone = response.xpath('.//a[contains(text(),"Bristol Property Centre")]/..//div[contains(@class,"phone")]/text()').extract_first()
        if landlord_phone:
            item_loader.add_value('landlord_phone', ''.join(landlord_phone.split(' ')[1:]))
        
        # Not Available in page source
        # landlord_email = response.xpath('.//a[contains(text(),"Bristol Property Centre")]/..//a[contains(@href,"mailto")]/text()').extract_first()
        # if landlord_email:
        #     item_loader.add_value('landlord_email',landlord_email)

        item_loader.add_value("external_source", "Bristolpropertycentre_PySpider_{}_{}".format(self.country, self.locale))
        self.position += 1
        item_loader.add_value("position", self.position)
        yield item_loader.load_item()
