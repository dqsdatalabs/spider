# -*- coding: utf-8 -*-
# Author: Praveen Chaudhary
# Team: Sabertooth
import js2xml
import lxml
import scrapy
from scrapy import Selector

from ..loaders import ListingLoader


class LincolnmilesCoUkPyspiderUnitedkingdomEnSpider(scrapy.Spider):
    name = 'lincolnmiles_co_uk'
    allowed_domains = ['lincolnmiles.co.uk']
    start_urls = ['https://www.lincolnmiles.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'

    def start_requests(self):
        self.position = 0
        start_urls = [
            {
                'url': 'https://www.lincolnmiles.co.uk/properties-to-let',
                'formdata': {
                    'filter_stype': '4',
                    'filter_order': 'p.price',
                    'filter_order_Dir': 'DESC',
                    'commit': '8',
                    'a8bcf745f4c74a98913230b9c05ecf2': '1',
                }},
        ]
        for url in start_urls:
            yield scrapy.FormRequest(url=url.get("url"),
                                     callback=self.parse,
                                     formdata=url.get('formdata'),
                                     meta={'response_url': url.get('url')})

    def parse(self, response, **kwargs):
        listings = response.xpath(
            './/div[@id="smallProps"]//div[@class="eapow-property-thumb-holder"]/a/@href').extract()
        for property_item in listings:
            yield scrapy.Request(
                url=f"https://www.lincolnmiles.co.uk{property_item}",
                callback=self.get_property_details,
                meta={'request_url': f"https://www.lincolnmiles.co.uk{property_item}"}
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_xpath('external_id', './/b[contains(text(),"Ref")]/parent::div/text()')
        item_loader.add_xpath('title', './/title/text()')
        item_loader.add_xpath('rent_string', './/small[@class="eapow-detail-price"]/text()')
        item_loader.add_xpath('description', './/ul[@id="starItem"]/following-sibling::p/text()')
        item_loader.add_xpath('images', './/div[@id="slider"]//li/img/@src')
        item_loader.add_xpath('floor_plan_images', './/div[@id="eapowfloorplanplug"]//img/@src')

        item_loader.add_value('landlord_name', 'Danny Miles - Lincoln Miles Estate Agents')
        item_loader.add_value('landlord_email', 'danny.miles@lincolnmiles.co.uk')
        item_loader.add_value('landlord_phone', '0845 257 7768')

        room_count = response.xpath(
            './/i[@class="propertyIcons-bedrooms"]/following-sibling::strong/text()').extract_first()
        if room_count:
            item_loader.add_value('room_count', room_count)
        bathroom_count = response.xpath(
            './/i[@class="propertyIcons-bathrooms"]/following-sibling::strong/text()').extract_first()
        if bathroom_count:
            item_loader.add_value('bathroom_count', bathroom_count)

        city_zip = response.xpath('.//div[@class="eapow-sidecol eapow-mainaddress"]/address/text()').extract_first()
        if city_zip:
            city_zip_list = city_zip.split(" ")
            city, zipcode = " ".join(city_zip_list[:-2]), " ".join(city_zip_list[-2:])
            street = response.xpath(
                './/div[@class="eapow-sidecol eapow-mainaddress"]/address/strong/text()').extract_first()
            address = f"{street}, {city}, {zipcode}"
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('address', address)
        else:
            address = response.xpath('.//div[@class="eapow-mainheader"]/h1/text()').extract_first()
            item_loader.add_value('address', address)

        javascript = response.xpath('.//script[contains(text(),"eapowPropertyMap")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            latitude = xml_selector.xpath('.//property [@name="lat"]/string/text()').extract_first()
            longitude = xml_selector.xpath('.//property [@name="lon"]/string/text()').extract_first()
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)

        # feature section
        features = " ".join(response.xpath('.//ul[@id="starItem"]/li/text()').extract())
        if "parking" in features.lower():
            item_loader.add_value('parking', True)

        if "furnished" in features.lower():
            item_loader.add_value('furnished', True)
        if "unfurnished" in features.lower():
            item_loader.add_value('furnished', True)

        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)

        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)

        if "swimming pool" in features.lower():
            item_loader.add_value('swimming_pool', True)

        if "dishwasher" in features.lower():
            item_loader.add_value('dishwasher', True)

        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)

        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
        house_types = ['chalet', 'bungalow', 'maison', 'house',
                       'home', ' villa ', 'cottage', 'semi-detached']
        studio_types = ["studio"]
        if any(i in item_loader.get_output_value('description').lower() for i in studio_types):
            item_loader.add_value('property_type', 'studio')
        elif any(i in item_loader.get_output_value('description').lower() for i in apartment_types):
            item_loader.add_value('property_type', 'apartment')
        elif any(i in item_loader.get_output_value('description').lower() for i in house_types):
            item_loader.add_value('property_type', 'house')
        else:
            return

        self.position += 1
        item_loader.add_value('position', self.position)

        item_loader.add_value("external_source",
                              "Lincolnmiles_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
