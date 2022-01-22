# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy, copy, urllib
from ..loaders import ListingLoader
from python_spiders.helper import extract_number_only, remove_white_spaces
import re


class IcsrealestateSpider(scrapy.Spider):
    name = 'ics-realestate_com' 
    allowed_domains=['www.ics-realestate.com'] 
    execution_type = 'testing'
    country = 'france' 
    locale = 'fr'
    thousand_separator='.'
    scale_separator=','
    api_url = "https://www.ics-realestate.com/fr-BE/List/PartialListEstate"
    params = {'EstatesList': 'System.Collections.Generic.List`1[Webulous.Immo.DD.WEntities.Estate]',
              'EstateListForNavigation': 'System.Collections.Generic.List`1[Webulous.Immo.DD.WEntities.Estate]',
              'Categories': 'System.Collections.Generic.List`1[System.Web.Mvc.SelectListItem]',
              'MinPrice': 0,
              'MaxPriceSlider': 10000,
              'ListID': 7,
              'SearchType': 'ToRent',
              'SearchTypeIntValue': 0,
              'SelectedCities': 'System.Collections.Generic.List`1[System.String]',
              'Cities': 'System.Collections.Generic.List`1[System.Web.Mvc.SelectListItem]',
              'SelectedRegion': 0,
              'SortParameter': None,
              'Furnished': False,
              'InvestmentEstate': False,
              'CurrentPage': 0,
              'MaxPage': 10,
              'EstateCount': 124,
              'SoldEstates': False,
              'RemoveSoldRentedOptionEstates': False,
              'List': 'Webulous.Immo.DD.CMSEntities.EstateListContentObject',
              'Page': 'Webulous.Immo.DD.CMSEntities.Page',
              'ContentZones': 'System.Collections.Generic.List`1[Webulous.Immo.DD.CMSEntities.ContentZone]',
              'DisplayMap': False,
              'MapZipMarkers': 'System.Collections.Generic.List`1[Webulous.Immo.DD.WEntities.MapZipMarker]',
              'EstateTotalCount': 0,
              'isMobileDevice': False,
              'SelectedCountries': 'System.Collections.Generic.List`1[System.String]',
              'Countries': 'System.Collections.Generic.List`1[System.Web.Mvc.SelectListItem]',
              'CountrySearch': 'Undefined'}
    position = 0

    def start_requests(self):
        start_urls = [self.api_url + "?" + urllib.parse.urlencode(self.params)]
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse, 
                                 meta={'request_url': url,
                                       'params': self.params})

    def parse(self, response, **kwargs):
        for property_url in response.xpath('.//a[@class="estate-card"]/@href').extract():
            yield scrapy.Request(
                url=response.urljoin(property_url),
                callback=self.get_property_details,
                meta={'request_url': response.urljoin(property_url)}
            )

        if len(response.xpath('.//a[@class="estate-card"]')) > 0:
            current_page = response.meta["params"]["CurrentPage"]
            params1 = copy.deepcopy(self.params)
            params1["CurrentPage"] = current_page + 1
            next_page_url = self.api_url + "?" + urllib.parse.urlencode(params1)
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'params': params1}
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        external_link = response.meta.get('request_url')
        item_loader.add_value('external_link', external_link)
        item_loader.add_xpath('external_id', './/th[contains(text(), "Référence")]/../td/text()')
        item_loader.add_xpath('title', './/title/text()')
        title = item_loader.get_output_value('title')
        property_type = response.xpath('.//th[contains(text(), "Catégorie")]/../td/text()').extract_first()
        apartment_types = ["lejlighed", "appartement", "apartment", "piso", "flat", "atico",
                           "penthouse", "duplex", "dakappartement", "triplex"]
        house_types = ['hus', 'chalet', 'bungalow', 'maison', 'house', 'home', 'villa', 'huis']
        studio_types = ["studio"]
        if property_type.lower() in apartment_types + house_types + studio_types:
            if property_type in apartment_types:
                item_loader.add_value('property_type', "apartment")
            elif property_type in house_types:
                item_loader.add_value('property_type', "house")
            elif property_type in studio_types:
                item_loader.add_value('property_type', 'studio')
            item_loader.add_xpath('description', './/*[contains(text(), "Description")]/../p/text()')
            description = item_loader.get_output_value('description')
            item_loader.add_xpath('address', './/*[contains(text(), "Adresse")]/following-sibling::p/text()')
            address = response.xpath('.//*[contains(text(), "Adresse")]/following-sibling::p/text()').extract_first()
            if address:
                item_loader.add_value('city', address.split()[-1])
                item_loader.add_value('zipcode', address.split()[-2])
            else:
                zip_city = title.split(' - ')[-1]
                city = re.search(r'(?<=\d.)\s.*',zip_city.split(' - ')[-1])
                item_loader.add_value('city',remove_white_spaces(city.group()))
                postcode = extract_number_only(zip_city,thousand_separator='.',scale_separator=',')
                if postcode:
                    item_loader.add_value('zipcode',postcode)
            item_loader.add_xpath('images', './/div[@class="owl-estate-photo"]/a/@href')
            item_loader.add_xpath('rent_string', './/h1[contains(@class, "estate-detail-intro__text")]/text()[last()]')

            
           
            deposit=response.xpath("//h2[.='Aspects financiers']/following-sibling::table/tbody//th[contains(.,'Garantie location')]/following-sibling::td/text()").get()
            if deposit:
                rent=response.xpath("//h1[contains(@class,'estate-detail-intro__text')]/text()[last()]").get()
                if rent:
                    rent=rent.replace("\n","").replace("€","").replace("\r","").replace(".","").strip()
                    item_loader.add_value("deposit",int(deposit)*int(rent))


            room_count = response.xpath('.//th[contains(text(), "Nombre de chambres")]/../td/text()').extract_first()
            if room_count is None or (room_count == '0' and property_type.upper() == "STUDIO"):
                item_loader.add_value('room_count', '1')
            elif room_count:
                item_loader.add_value('room_count', room_count)
            item_loader.add_xpath('bathroom_count', './/th[contains(text(), "Nombre de salle de bain")]/../td/text()')
            item_loader.add_xpath('square_meters', './/th[contains(text(), "Surface habitable")]/../td/text()')
            item_loader.add_xpath('energy_label', './/th[contains(text(), "PEB (classe)")]/../td/text()')
            utilities = response.xpath('.//th[contains(text(), "Charges (€) (montant)")]/../td/text()').extract_first()
            if utilities:
                item_loader.add_value('utilities',extract_number_only(utilities))
            else:
                utility_charge = re.search(r'(?<=charge).{0,2}\d+',description.lower())
                if utility_charge:
                    item_loader.add_value('utilities',extract_number_only(utility_charge.group(),thousand_separator='.',scale_separator=','))
                      

            terrace = response.xpath('.//th[contains(text(), "Terrasse")]/../td/text()').extract_first()
            if terrace and terrace == "Oui":
                item_loader.add_value('terrace', True)
            elif terrace and terrace == "Non":
                item_loader.add_value('terrace', False)

            # parking
            parking = response.xpath('.//th[contains(text(), "Parking")]/../td/text()').extract_first()
            if parking and parking == "Oui":
                item_loader.add_value('parking', True)
            elif parking and parking == "Non":
                item_loader.add_value('parking', False)

            # furnished
            furnished = response.xpath('.//th[contains(text(), "Meublé")]/../td/text()').extract_first()
            if furnished and furnished == "Oui":
                item_loader.add_value('furnished', True)
            elif furnished and furnished == "Non":
                item_loader.add_value('furnished', False)

            # pets_allowed
            pets_allowed = response.xpath('.//th[contains(text(), "Animaux domestiques")]/../td/text()').extract_first()
            if pets_allowed and pets_allowed == "Oui":
                item_loader.add_value('pets_allowed', True)
            elif pets_allowed and pets_allowed == "Non":
                item_loader.add_value('pets_allowed', False)

            # washing_machine
            washing_machine = response.xpath('.//th[contains(text(), "Machine à laver le linge")]/../td/text()').extract_first()
            if washing_machine and washing_machine == "Oui":
                item_loader.add_value('washing_machine', True)
            elif washing_machine and washing_machine == "Non":
                item_loader.add_value('washing_machine', False)

            # dishwasher
            dishwasher = response.xpath('.//th[contains(text(), "Lave-vaisselle")]/../td/text()').extract_first()
            if dishwasher and dishwasher == "Oui":
                item_loader.add_value('dishwasher', True)
            elif dishwasher and dishwasher == "Non":
                item_loader.add_value('dishwasher', False)

            # balcony
            balcony = response.xpath('.//th[contains(text(), "Balcon")]/../td/text()').extract_first()
            if balcony and balcony == "Oui":
                item_loader.add_value('balcony', True)
            elif balcony and balcony in ["Non", "0"]:
                item_loader.add_value('balcony', False)

            # swimming_pool
            # example: https://www.ics-realestate.com/fr/bien/a-louer/maison/-roquebrune-/3398305
            swimming_pool = response.xpath('.//th[contains(text(), "Piscine")]/../td/text()').extract_first()
            if swimming_pool and swimming_pool == "Oui":
                item_loader.add_value('swimming_pool', True)
            elif swimming_pool and swimming_pool == "Non":
                item_loader.add_value('swimming_pool', False)
                
            # elevator
            elevator = response.xpath('.//th[contains(text(), "Ascenseur")]/../td/text()').extract_first()
            if elevator and elevator == "Oui":
                item_loader.add_value('elevator', True)
            elif elevator and elevator == "Non":
                item_loader.add_value('elevator', False)
                
            item_loader.add_value("external_source", "Ics_realestate_com_PySpider_{}_{}".format(self.country, self.locale))
            item_loader.add_value('landlord_name', 'ics-realestate')
            item_loader.add_value('landlord_email', 'audrey@ics-realestate.com')
            item_loader.add_value('landlord_phone', '+32 2 428 57 67')

            self.position += 1
            item_loader.add_value('position', self.position)
            yield item_loader.load_item()
