# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import format_date
import json
import re
import dateparser

class ImmobossBeSpider(scrapy.Spider):
    name = "immoboss_be"
    allowed_domains = ["immoboss.be"]
    start_urls = (
        'http://www.immoboss.be/',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    thousand_separator = '.'
    scale_separator = ','
    position = 0
    api_url = 'https://immoboss.be/Modules/ZoekModule/RESTService/SearchService.svc/GetPropertiesJSON/0/0'

    def start_requests(self):
        body = {'CheckedTypes': "0",
                'City': "0",
                'CompanyID': "0",
                'CountryExclude': "0",
                'CountryInclude': "0",
                'ExtraSQL': "0",
                'ExtraSQLFilters': "0",
                'FilterOutTypes': "0",
                'Language': "NL",
                'MaxBedrooms': "0",
                'MaxPrice': "0",
                'MaxSurface': "0",
                'MaxSurfaceGround': "0",
                'MediaID': "0",
                'MinBedrooms': "0",
                'MinPrice': "0",
                'MinSurface': "0",
                'MinSurfaceGround': "0",
                'NavigationItem': "0",
                'NumResults': "12",
                'OrderBy': 1,
                'PageName': "0",
                'PriceClass': "0",
                'PropertyID': "0",
                'PropertyName': "0",
                'Radius': "0",
                'Region': "0",
                'SQLType': "3",
                'ShowChildrenInsteadOfProject': 'false',
                'ShowProjects': 'false',
                'SliderItem': "0",
                'SliderStep': "0",
                'SortField': "1",
                'StartIndex': 1,
                'Token': "KFRGQJJNVVWCTYRIFYSTBBTITXFPLOHCCGBCCOXPXLIILKTIUO",
                'Transaction': "2",
                'Type': "1",
                'UsePriceClass': 'false',
                'bedrooms': '0',
                'currentPage': "0",
                'homeSearch': "0",
                'investment': 'false',
                'latitude': "0",
                'longitude': "0",
                'menuIDUmbraco': "0",
                'newbuilding': 'false',
                'officeID': "0",
                'state': "0",
                'useCheckBoxes': 'false',}
                
        property_types = [{'Type': '1',
                           'property_type': 'apartment'}]

        for property_type in property_types:
            body['Type'] = property_type['Type']
            yield scrapy.http.JsonRequest(url=self.api_url,
                                          callback=self.parse,
                                          data=body,
                                          meta={'data': body,
                                                'property_type': property_type.get('property_type')})

    def parse(self, response, **kwargs):
        response_json = json.loads(response.text)
        # print(response_json[0])
        for item in response_json:
            yield scrapy.Request(url=response.urljoin(item['Property_URL']),
                                 callback=self.get_property_details,
                                 meta={'request_url': response.urljoin(item['Property_URL']),
                                       'rent_string': item['Property_Price'],
                                       'external_id': item['Property_Reference'],
                                       'latitude': item['Property_Lat'],
                                       'longitude': item['Property_Lon'],
                                       'zipcode': item['Property_Zip'],
                                       'room_count': item['bedrooms'],
                                       'description': item['Property_Description'],
                                       'title': item['Property_Title']})
    
        if len(response_json) == 12:
            data = response.meta.get('data')
            data['StartIndex'] = data['StartIndex']+1
            yield scrapy.http.JsonRequest(url=self.api_url,
                                          callback=self.parse,
                                          data=data,
                                          meta={'data': data,
                                                'property_type': response.meta.get('property_type')})

    def get_property_details(self, response, **kwargs):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.meta.get('request_url'))
        desc = response.meta.get('description')
        if desc:
            item_loader.add_value("description", desc)

        rent = response.meta.get('rent_string')
        if rent and "prijs " not in rent:
            item_loader.add_value("rent_string", response.meta.get('rent_string').replace(" ","").replace("\xa0",""))
        else:
            if desc and "prijs " in desc.lower():
                item_loader.add_value('rent_string', desc.lower().split("prijs ")[-1].split("maan")[0])
        item_loader.add_value("external_id", response.meta.get('external_id'))
        item_loader.add_value("latitude", str(response.meta.get('latitude')))
        item_loader.add_value("longitude", str(response.meta.get('longitude')))
        item_loader.add_value("zipcode", response.meta.get('zipcode'))
        if int(response.meta.get('room_count')) > 0:
            item_loader.add_value("room_count", response.meta.get('room_count'))
        else:
            item_loader.add_value("room_count", '1')

        # property_type
        description = response.meta.get("description") or " "
        title = response.meta.get("title") or " "
        if "studio" in description.lower() or "studio" in title.lower():
            item_loader.add_value("property_type", "studio")
        elif "appartement" in description.lower() or "appartement" in title.lower():
            item_loader.add_value('property_type', "apartment")
        elif "penthouse" in description.lower() or "penthouse" in title.lower():
            item_loader.add_value('property_type', "house")
        item_loader.add_value("title", response.meta.get('title'))

        item_loader.add_xpath("square_meters", './/td[contains(text(),"Bewoonbare opp.")]/following-sibling::td/text()')
        if not item_loader.get_output_value('square_meters'):
            item_loader.add_xpath("square_meters", './/td[contains(text(),"Totale opp.")]/following-sibling::td/text()')
        item_loader.add_xpath("address", './/td[contains(text(),"Adres")]/following-sibling::td/text()')
        item_loader.add_value("city", item_loader.get_output_value('address').split()[-1])
        item_loader.add_xpath("utilities", './/td[contains(text(),"Maandelijkse lasten")]/following-sibling::td/text()')

        item_loader.add_xpath("images", './/meta[@property="og:image"]/@content')

        pets_allowed = response.xpath('.//td[contains(text(),"Huisdieren toegelaten")]/following-sibling::td/text()').extract_first()
        if pets_allowed:
            if pets_allowed not in ['nee']:
                item_loader.add_value('pets_allowed', True)
            else:
                item_loader.add_value('pets_allowed', False)

        available_date = response.xpath('.//td[contains(text(),"Beschikbaar vanaf")]/following-sibling::td/text()').extract_first()
        if available_date:
            available_date = re.findall(r'\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4}', available_date)
            if available_date:
                item_loader.add_value('available_date', format_date(available_date[0]))
            else:
                available_date = response.xpath("//div[@class='descriptionInfo']/text()[contains(.,'Beschikbaar vanaf')]").extract_first()
                if available_date:
                    date_parsed = dateparser.parse(available_date.split(" vanaf ")[-1].replace("!","").strip(), date_formats=["%d %m %Y"])
                    if date_parsed:
                        item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        EPC = response.xpath('.//td[contains(text(),"EPC")]/following-sibling::td/text()').extract()[-1]
        if EPC and 'In aanvraag' not in EPC:
            item_loader.add_value('energy_label', EPC)

        terrace = response.xpath('.//th[contains(text(),"Terras")]').extract_first()
        if terrace:
            item_loader.add_value('terrace', True)

        parking = response.xpath('.//th[contains(text(),"Garage")]').extract_first()
        if parking:
            item_loader.add_value('parking', True)

        bathroom_count = response.xpath('.//th[contains(text(),"Badkamer")]').extract()
        if len(bathroom_count) > 0:
            item_loader.add_value('bathroom_count', str(len(bathroom_count)))

        item_loader.add_value('landlord_name', 'ImmoBoss')
        item_loader.add_xpath('landlord_phone', './/div[@id="content"]//i[contains(@class,"phone")]/following-sibling::text()')
        item_loader.add_value("external_source", "Immoboss_PySpider_{}_{}".format(self.country, self.locale))
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
