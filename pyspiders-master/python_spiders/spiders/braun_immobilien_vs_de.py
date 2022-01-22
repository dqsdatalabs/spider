# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import re

import scrapy
from ..loaders import ListingLoader
from ..helper import *


class BraunImmobilienVsDeSpider(scrapy.Spider):
    name = "braun_immobilien_vs_de"
    start_urls = ['https://braun-immobilien-vs.de/immobilien/']
    allowed_domains = ["braun-immobilien-vs.de"]
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        n = len(response.xpath(
            '//*[@id="post-2120"]/div/div/div/div[2]/div[1]/div[1]/div/div/div[3]/div[2]/div/div[*]/div/div/div[2]/div/div[2]/div/a/@href').extract())
        for i in range(1, n + 1):
            state = response.xpath(
                f'//*[@id="post-2120"]/div/div/div/div[2]/div[1]/div[1]/div/div/div[3]/div[2]/div/div[{i}]/div/div/div[1]/div/div[1]/div/div/div/div/div/p/span[2]/text()').get()
            forbidden_states = ['Verkauft', 'vermietet', 'Reserviert']
            url = response.xpath(
                f'//*[@id="post-2120"]/div/div/div/div[2]/div[1]/div[1]/div/div/div[3]/div[2]/div/div[{i}]/div/div/div[2]/div/div[2]/div/a/@href').get()
            if state and state in forbidden_states:
                continue
            else:
                yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # Description
        desc = response.css('.et_pb_post_content_0_tb_body p::text').extract()
        desc = ' '.join(desc)
        desc = re.sub(r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})',
                             '', desc)
        desc = re.sub(r'[\S]+\.(net|com|org|info|edu|gov|uk|de|ca|jp|fr|au|us|ru|ch|it|nel|se|no|es|mil)[\S]*\s?', '', desc)
        description = re.sub(r"[_,.*+(){}';@#?!&$/-]+\ *", " ", desc)
        description = re.sub(r" +", " ", description)

        if 'mietwohnung' not in description.lower():
            return
        else:
            property_type = 'apartment'

        # details
        labels = response.css('.et_pb_de_mach_acf_item_47_tb_body p strong::text').extract()
        values = response.css('.et_pb_de_mach_acf_item_47_tb_body p::text').extract()[0::2]
        label_values = dict(zip(labels, values))

        # rent, id, balcony, square_meters, terrace, deposit, parking, room_count
        if 'Kaltmiete:' in label_values.keys():
            rent = int(float(label_values['Kaltmiete:'].split()[0].replace('.','').replace(',','.').strip()))
        else:
            return

        room_count = 1
        if 'Zimmer:' in label_values.keys():
            room_count = int(re.search(r'\d', label_values['Zimmer:'])[0])

        square_meters = None
        if 'Wohnfläche:' in label_values.keys():
            square_meters = int(float(label_values['Wohnfläche:'].split()[1]))

        deposit = None
        if 'Kaution:' in label_values.keys():
            deposit = int(float(label_values['Kaution:'].split(',')[0].replace('.', '').strip()))

        external_id = None
        if 'Referenznummer:' in label_values.keys():
            external_id = label_values['Referenznummer:'].strip()

        floor = None
        if 'Etage:' in label_values.keys():
            if re.search(r'\d', label_values['Etage:']):
                floor = re.search(r'\d', label_values['Etage:'])[0]

        other = re.sub(r"[_,.*+(){}';@#?!&$/-]+\ *", " ", label_values['Sonstiges:'])

        balcony = None
        if re.search('balkone', other.lower()) or re.search('balkone', description.lower()):
            balcony = True

        terrace = None
        if re.search('terrasse', other.lower()) or re.search('terrasse', description.lower()):
            terrace = True

        parking = None
        if re.search('garage', other.lower()) or 'Garage:' in label_values.keys() or re.search('garage', description.lower()):
            parking = True

        elevator = None
        if re.search('personenaufzug', other.lower()) or re.search('personenaufzug', description.lower()):
            elevator = True

        # energy label
        energy_label = None
        energy_desc = dict(zip(response.css('.dmach-acf-label::text').extract(), response.css('.dmach-acf-value::text').extract()))
        if 'Energieeff.kl.' in energy_desc.keys():
            energy_label = energy_desc['Energieeff.kl.']

        # Utilities
        warm_rent = int(float(response.css('.et_pb_post_content_0_tb_body br+ strong::text').get().split()[-3].replace(',', '.')))

        utilities = None
        if warm_rent:
            utilities = warm_rent - rent

        # Title
        title = response.css('.entry-title::text').get()

        # washing machine
        x = ' '.join(response.css('.et_pb_de_mach_acf_item_13_tb_body p::text').extract()).lower()
        washing_machine = None
        if 'wasch' in description.lower() or 'wasch' in x:
            washing_machine = True

        # images
        im = response.css('img::attr(src)').extract()
        images = []
        for i in im:
            if '.JPG' in i or '.jpg' in i:
                images.append(i)
        images = list(set(images))

        # landlord info
        landlord_name = response.css('p:nth-child(1) strong::text').get()
        landlord_number = response.css('.et_pb_text_6_tb_body a::text').get()
        landlord_email = 'info@kerstinbraun-immobilien.de'

        # posistion
        longitude = None
        latitude = None
        zipcode = None
        city = None
        address = None
        pos = response.css('.et_pb_de_mach_acf_item_12_tb_body p::text').extract()
        position = re.sub(r"[_,.*+(){}';@#?!&$/-]+\ *", " ", ' '.join(pos))
        for i in self.lng.keys():
            if i in position:
                longitude = self.lng[i]
                latitude = self.lat[i]
                zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
                break

        # Bathroom
        bathroom_count = None
        if 'Badezimmer' in energy_desc.keys():
            bathroom_count = energy_desc['Badezimmer']

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        item_loader.add_value("deposit", deposit)  # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()

    lng = {'Berlin': 13.3833, 'Hamburg': 10.0, 'Munich': 11.5755, 'Cologne': 6.9578, 'Frankfurt': 8.6797, 'Bremen': 8.7975,
     'Düsseldorf': 6.7724, 'Stuttgart': 9.1775, 'Leipzig': 12.3833, 'Dortmund': 7.4653, 'Essen': 7.0131,
     'Dresden': 13.74, 'Hannover': 9.7386, 'Nuremberg': 11.0775, 'Duisburg': 6.7611, 'Bochum': 7.2167,
     'Wuppertal': 7.1833, 'Bielefeld': 8.5333, 'Bonn': 7.0997, 'Münster': 7.6256, 'Karlsruhe': 8.4, 'Mannheim': 8.4661,
     'Augsburg': 10.8983, 'Kassel': 9.4912, 'Wiesbaden': 8.24, 'Mönchengladbach': 6.4333, 'Gelsenkirchen': 7.1,
     'Braunschweig': 10.5211, 'Aachen': 6.0838, 'Kiel': 10.1394, 'Chemnitz': 12.9167, 'Halle': 11.9697,
     'Magdeburg': 11.6292, 'Freiburg im Breisgau': 7.8497, 'Krefeld': 6.5667, 'Mainz': 8.2667, 'Lübeck': 10.6864,
     'Oberhausen': 6.8514, 'Rostock': 12.1333, 'Erfurt': 11.0328, 'Hagen': 7.475, 'Potsdam': 13.0667,
     'Saarbrücken': 7.0, 'Hamm': 7.8167, 'Ludwigshafen': 8.4353, 'Mülheim': 6.8825, 'Oldenburg': 8.2139,
     'Osnabrück': 8.0431, 'Leverkusen': 6.9833, 'Heidelberg': 8.71, 'Solingen': 7.0833, 'Darmstadt': 8.65,
     'Herne': 7.219, 'Neuss': 6.6939, 'Regensburg': 12.0833, 'Paderborn': 8.7667, 'Ingolstadt': 11.4261,
     'Würzburg': 9.9294, 'Fürth': 10.9903, 'Ulm': 9.9916, 'Heilbronn': 9.218, 'Pforzheim': 8.705, 'Wolfsburg': 10.7872,
     'Göttingen': 9.9356, 'Bottrop': 6.9253, 'Reutlingen': 9.2167, 'Koblenz': 7.5978, 'Bremerhaven': 8.5833,
     'Recklinghausen': 7.2, 'Bergisch Gladbach': 7.1367, 'Erlangen': 11.0044, 'Jena': 11.5864, 'Remscheid': 7.1872,
     'Trier': 6.6414, 'Salzgitter': 10.3593, 'Moers': 6.6197, 'Siegen': 8.0167, 'Hildesheim': 9.95, 'Cottbus': 14.3342,
     'Gütersloh': 8.3833, 'Kaiserslautern': 7.7689, 'Witten': 7.3333, 'Hanau': 8.9169, 'Schwerin': 11.4167,
     'Gera': 12.0824, 'Esslingen': 9.3108, 'Ludwigsburg': 9.1919, 'Iserlohn': 7.6667, 'Düren': 6.4833,
     'Tübingen': 9.0556, 'Flensburg': 9.4367, 'Zwickau': 12.4961, 'Gießen': 8.6667, 'Ratingen': 6.85, 'Lünen': 7.5167,
     'Villingen-Schwenningen': 8.4586, 'Konstanz': 9.1753, 'Marl': 7.1167, 'Worms': 8.3653, 'Velbert': 7.0416,
     'Minden': 8.9167, 'Dessau-Roßlau': 12.2333, 'Neumünster': 9.99, 'Norderstedt': 10.0103, 'Delmenhorst': 8.6317,
     'Bamberg': 10.8917, 'Viersen': 6.3917, 'Marburg': 8.7667, 'Rheine': 7.4333, 'Wilhelmshaven': 8.1333,
     'Gladbeck': 6.9827, 'Lüneburg': 10.4144, 'Troisdorf': 7.1556, 'Dorsten': 6.9642, 'Bayreuth': 11.5783,
     'Detmold': 8.8833, 'Arnsberg': 8.0644, 'Castrop-Rauxel': 7.3167, 'Lüdenscheid': 7.6273, 'Landshut': 12.1508,
     'Brandenburg': 12.5561, 'Bocholt': 6.6167, 'Aschaffenburg': 9.1478, 'Celle': 10.0825, 'Kempten': 10.3167,
     'Fulda': 9.6775, 'Aalen': 10.0936, 'Lippstadt': 8.35, 'Dinslaken': 6.7333, 'Herford': 8.6734, 'Kerpen': 6.6961,
     'Rüsselsheim': 8.4119, 'Weimar': 11.3167, 'Plauen': 12.1383, 'Sindelfingen': 9.0028, 'Neuwied': 7.4614,
     'Dormagen': 6.84, 'Neubrandenburg': 13.2608, 'Grevenbroich': 6.5875, 'Rosenheim': 12.1289, 'Herten': 7.1333,
     'Bergheim': 6.65, 'Schwäbisch Gmünd': 9.8, 'Friedrichshafen': 9.4792, 'Garbsen': 9.5981, 'Wesel': 6.6178,
     'Hürth': 6.8761, 'Offenburg': 7.9408, 'Stralsund': 13.0819, 'Greifswald': 13.3833, 'Langenfeld': 6.95,
     'Neu-Ulm': 10.0, 'Unna': 7.6889, 'Euskirchen': 6.7873, 'Frankfurt (Oder)': 14.55, 'Göppingen': 9.6528,
     'Hameln': 9.36, 'Stolberg': 6.2333, 'Eschweiler': 6.2833, 'Görlitz': 14.9872, 'Langenhagen': 9.74,
     'Meerbusch': 6.6667, 'Sankt Augustin': 7.1867, 'Hilden': 6.9394, 'Waiblingen': 9.3169, 'Baden-Baden': 8.2408,
     'Hattingen': 7.1858, 'Lingen': 7.3172, 'Bad Homburg': 8.6105, 'Bad Salzuflen': 8.7506, 'Pulheim': 6.8,
     'Schweinfurt': 10.2333, 'Nordhorn': 7.0667, 'Neustadt': 8.8437, 'Wetzlar': 8.5, 'Ahlen': 7.8911, 'Frechen': 6.8167,
     'Passau': 13.4667, 'Wolfenbüttel': 10.5369, 'Ibbenbüren': 7.7167, 'Kleve': 6.14, 'Bad Kreuznach': 7.8669,
     'Goslar': 10.4292, 'Gummersbach': 7.5667, 'Ravensburg': 9.6114, 'Willich': 6.5492, 'Speyer': 8.4311,
     'Böblingen': 9.0, 'Peine': 10.2336, 'Emden': 7.2061, 'Elmshorn': 9.6536, 'Erftstadt': 6.7667, 'Rastatt': 8.2031,
     'Heidenheim': 10.1544, 'Lörrach': 7.6614, 'Leonberg': 9.0131, 'Bergkamen': 7.6333, 'Bad Oeynhausen': 8.8,
     'Freising': 11.7489, 'Rheda-Wiedenbrück': 8.3, 'Cuxhaven': 8.7, 'Bornheim': 7.005, 'Gronau': 7.0417,
     'Straubing': 12.5758, 'Stade': 9.4764, 'Soest': 8.1092, 'Dachau': 11.4342, 'Alsdorf': 6.1615, 'Landau': 8.1231,
     'Dülmen': 7.2783, 'Melle': 8.3361, 'Neunkirchen': 7.1799, 'Herzogenrath': 6.1, 'Schwerte': 7.5653,
     'Oberursel': 8.5769, 'Wittenberg': 12.6484, 'Hof': 11.9167, 'Filderstadt': 9.2167, 'Gotha': 10.7183,
     'Fellbach': 9.2758, 'Bünde': 8.6, 'Albstadt': 9.0239, 'Weinheim': 8.6697, 'Rodgau': 8.8841, 'Bruchsal': 8.6,
     'Oranienburg': 13.2369, 'Brühl': 6.9, 'Erkrath': 6.9147, 'Neustadt am Rübenberge': 9.4636, 'Lehrte': 9.9769,
     'Kaufbeuren': 10.6225, 'Falkensee': 13.0917, 'Memmingen': 10.1811, 'Kaarst': 6.6273, 'Erkelenz': 6.3156,
     'Pinneberg': 9.8009, 'Bietigheim-Bissingen': 9.1333, 'Kamen': 7.6653, 'Wismar': 11.465, 'Borken': 6.8583,
     'Gifhorn': 10.5464, 'Nettetal': 6.2833, 'Eisenach': 10.3244, 'Dreieich': 8.6961, 'Aurich': 7.4836,
     'Amberg': 11.8483, 'Heinsberg': 6.0964, 'Ansbach': 10.5833, 'Homburg': 7.3333, 'Nordhausen': 10.7911,
     'Wunstorf': 9.4359, 'Seevetal': 10.0333, 'Siegburg': 7.2044, 'Laatzen': 9.8133, 'Germering': 11.3667,
     'Coburg': 10.9579, 'Königswinter': 7.1833, 'Nürtingen': 9.3353, 'Freiberg': 13.3428, 'Schwabach': 11.0208,
     'Lemgo': 8.9043, 'Kirchheim unter Teck': 9.4511, 'Bensheim': 8.6228, 'Schwäbisch Hall': 9.7375,
     'Weißenfels': 11.9667, 'Pirmasens': 7.6, 'Eberswalde': 13.8331, 'Halberstadt': 11.0467, 'Buxtehude': 9.7011,
     'Leinfelden-Echterdingen': 9.1428, 'Neumarkt': 11.4667, 'Hückelhoven': 6.2197, 'Hofheim': 8.4447, 'Löhne': 8.7,
     'Schorndorf': 9.5333, 'Stendal': 11.85, 'Freital': 13.65, 'Völklingen': 6.8333, 'Ettlingen': 8.4,
     'Ostfildern': 9.2631, 'Maintal': 8.8333, 'Buchholz in der Nordheide': 9.8621, 'Ahaus': 7.0134, 'Ilmenau': 10.9142,
     'Mettmann': 6.9667, 'Würselen': 6.1275, 'Bitterfeld': 12.3167, 'Bautzen': 14.4239, 'Pirna': 13.9403,
     'Niederkassel': 7.0333, 'Langen': 8.6803, 'Greven': 7.6083, 'Fürstenfeldbruck': 11.2556, 'Neu Isenburg': 8.6971,
     'Papenburg': 7.4, 'Kamp-Lintfort': 6.5333, 'Backnang': 9.4306, 'Warendorf': 7.9933, 'Königs Wusterhausen': 13.625,
     'Suhl': 10.6931, 'Beckum': 8.0408, 'Erding': 11.9082, 'Coesfeld': 7.1675, 'Mühlhausen': 10.45, 'Wesseling': 6.9786,
     'Kehl': 7.8089, 'Emsdetten': 7.5344, 'Tuttlingen': 8.8233, 'Sankt Ingbert': 7.1167, 'Porta Westfalica': 8.9333,
     'Sinsheim': 8.8833, 'Meppen': 7.291, 'Limburg': 8.0667, 'Ingelheim': 8.0564, 'Lage': 8.8, 'Cloppenburg': 8.0439,
     'Winsen': 10.2167, 'Mörfelden-Walldorf': 8.5661, 'Wermelskirchen': 7.2051, 'Datteln': 7.3417, 'Kempen': 6.4194,
     'Saarlouis': 6.75, 'Leer': 7.4528, 'Seelze': 9.5981, 'Crailsheim': 10.0706, 'Barsinghausen': 9.4606,
     'Balingen': 8.8506, 'Zweibrücken': 7.3608, 'Viernheim': 8.5792, 'Steinfurt': 7.3366, 'Merseburg': 11.9928,
     'Hemer': 7.7667, 'Dietzenbach': 8.7756, 'Radebeul': 13.67, 'Bad Vilbel': 8.7361, 'Geldern': 6.3325, 'Goch': 6.1619,
     'Kornwestheim': 9.185, 'Stuhr': 8.75, 'Uelzen': 10.5658, 'Deggendorf': 12.9644, 'Wedel': 9.7,
     'Ahrensburg': 10.2411, 'Rheinfelden (Baden)': 7.7917, 'Korschenbroich': 6.5167, 'Biberach': 9.7886,
     'Wernigerode': 10.7853, 'Bernburg': 11.7333, 'Jülich': 6.3583, 'Lampertheim': 8.4671, 'Vechta': 8.2886,
     'Naumburg': 11.8098, 'Forchheim': 11.0581, 'Bad Nauheim': 8.75, 'Hoyerswerda': 14.25, 'Altenburg': 12.4333,
     'Delbrück': 8.5667, 'Fürstenwalde': 14.0667, 'Achim': 9.033, 'Itzehoe': 9.5164, 'Georgsmarienhütte': 8.0447,
     'Herrenberg': 8.8708, 'Oer-Erkenschwick': 7.2508, 'Radolfzell am Bodensee': 8.9697, 'Kreuztal': 8.0064,
     'Ganderkesee': 8.5483, 'Rheinberg': 6.6006, 'Bramsche': 7.9728, 'Neuruppin': 12.8, 'Einbeck': 9.8667,
     'Werl': 7.9139, 'Schönebeck': 11.75, 'Burgdorf': 10.0078, 'Gevelsberg': 7.3559, 'Weyhe': 8.8733,
     'Geesthacht': 10.3675, 'Haan': 7.0131, 'Lohmar': 7.2166, 'Osterholz-Scharmbeck': 8.7947, 'Weil am Rhein': 7.6108,
     'Ennepetal': 7.3425, 'Riesa': 13.2939, 'Taunusstein': 8.1606, 'Andernach': 7.4017, 'Meschede': 8.2836,
     'Schwedt (Oder)': 14.2831, 'Friedberg': 8.755, 'Bad Hersfeld': 9.7067, 'Gaggenau': 8.3194, 'Merzig': 6.6312,
     'Neuburg': 11.1833, 'Werve': 7.6167, 'Vaihingen an der Enz': 8.9564, 'Rietberg': 8.4333, 'Saalfeld': 11.3542,
     'Bretten': 8.7061, 'Waltrop': 7.3972, 'Oelde': 8.1436, 'Tönisvorst': 6.4931, 'Güstrow': 12.1764,
     'Landsberg': 10.8689, 'Northeim': 10.0011, 'Kelkheim (Taunus)': 8.4525, 'Springe': 9.55,
     'Unterschleißheim': 11.5667, 'Bühl': 8.135, 'Schwandorf': 12.0993, 'Höxter': 9.3667, 'Rösrath': 7.1833,
     'Bad Zwischenahn': 8.0097, 'Schwelm': 7.2972, 'Rendsburg': 9.6644, 'Winnenden': 9.3978,
     'Bad Neuenahr-Ahrweiler': 7.1133, 'Grimma': 12.7253, 'Wegberg': 6.2667, 'Geislingen an der Steige': 9.8306,
     'Königsbrunn': 10.8908, 'Henstedt-Ulzburg': 10.0, 'Meißen': 13.4775, 'Leichlingen': 7.0167, 'Kevelaer': 6.25,
     'Zeitz': 12.1383, 'Emmendingen': 7.8492, 'Blankenfelde': 13.4, 'Sundern': 8.0, 'Baunatal': 9.4183,
     'Reinbek': 10.2483, 'Mechernich': 6.65, 'Hattersheim': 8.4862, 'Wetter (Ruhr)': 7.395, 'Griesheim': 8.5525,
     'Arnstadt': 10.9464, 'Aschersleben': 11.4667, 'Geilenkirchen': 6.1194, 'Rheinbach': 6.9491, 'Overath': 7.2839,
     'Baesweiler': 6.1833, 'Leimen': 8.6911, 'Wangen im Allgäu': 9.8342, 'Schloß Holte-Stukenbrock': 8.6167,
     'Lohne': 8.2386, 'Wiesloch': 8.6983, 'Hamminkeln': 6.5908, 'Strausberg': 13.8814, 'Lauf': 11.2772,
     'Neckarsulm': 9.2244, 'Heiligenhaus': 6.971, 'Sangerhausen': 11.3, 'Hennigsdorf': 13.2036,
     'Ehingen an der Donau': 9.7236, 'Butzbach': 8.6622, 'Nordenham': 8.4667, 'Hohen Neuendorf': 13.2831,
     'Ludwigsfelde': 13.2667, 'Mühlacker': 8.8392, 'Heppenheim': 8.645, 'Selm': 7.4833, 'Weiterstadt': 8.6,
     'Pfaffenhofen': 11.5167, 'Kulmbach': 11.4333, 'Sankt Wendel': 7.1667, 'Teltow': 13.2706,
     'Bad Honnef am Rhein': 7.2269, 'Helmstedt': 11.0106, 'Bingen am Rhein': 7.895, 'Achern': 8.0739, 'Zirndorf': 10.95,
     'Roth': 11.0911, 'Lennestadt': 8.0681, 'Verl': 8.5167, 'Lindau': 9.6839, 'Lübbecke': 8.6231, 'Rinteln': 9.0814,
     'Brilon': 8.5678, 'Staßfurt': 11.5667, 'Zittau': 14.8072, 'Plettenberg': 7.8715, 'Groß-Gerau': 8.4818,
     'Schleswig': 9.5697, 'Geretsried': 11.4667, 'Rottweil': 8.6247, 'Friedrichsdorf': 8.6418, 'Petershagen': 8.9667,
     'Pfungstadt': 8.6044, 'Bad Oldesloe': 10.3742, 'Olpe': 7.8514, 'Rathenow': 12.3333, 'Senftenberg': 14.0167,
     'Meiningen': 10.4167, 'Waldshut-Tiengen': 8.2144, 'Sonneberg': 11.1667, 'Salzwedel': 11.15, 'Calw': 8.7375,
     'Korbach': 8.8731, 'Starnberg': 11.3406, 'Freudenstadt': 8.4111, 'Mosbach': 9.1333, 'Husum': 9.0511,
     'Westerstede': 7.9167, 'Burg': 11.855, 'Weilheim': 11.1333, 'Bad Kissingen': 10.0667, 'Stadthagen': 9.2069,
     'Apolda': 11.5139, 'Heide': 9.0933, 'Osterode': 10.2522, 'Kitzingen': 10.1667, 'Sonthofen': 10.2817,
     'Sondershausen': 10.8667, 'Aichach': 11.1333, 'Germersheim': 8.3667, 'Günzburg': 10.2711, 'Luckenwalde': 13.1667,
     'Traunstein': 12.6433, 'Mühldorf': 12.5228, 'Wittmund': 7.7808, 'Bad Salzungen': 10.2333, 'Lichtenfels': 11.0333,
     'Greiz': 12.1997, 'Donauwörth': 10.777, 'Torgau': 13.0056, 'Holzminden': 9.4483, 'Dingolfing': 12.5,
     'Annaberg-Buchholz': 13.0022, 'Eschwege': 10.0528, 'Haldensleben': 11.4167, 'Sömmerda': 11.1169,
     'Prenzlau': 13.8667, 'Wittlich': 6.8897, 'Dillingen': 10.4667, 'Bad Tölz': 11.5567, 'Marktoberdorf': 10.6167,
     'Alzey': 8.1161, 'Bad Dürkheim': 8.1724, 'Weißenburg': 10.9719, 'Bad Reichenhall': 12.8769,
     'Forst (Lausitz)': 14.6333, 'Parchim': 11.8333, 'Sigmaringen': 9.2164, 'Bad Segeberg': 10.3089,
     'Heilbad Heiligenstadt': 10.1344, 'Eutin': 10.6181, 'Cham': 12.6658, 'Diepholz': 8.3711, 'Kronach': 11.3281,
     'Kelheim': 11.8667, 'Bad Neustadt': 10.2161, 'Künzelsau': 9.6833, 'Mindelheim': 10.4667, 'Brake': 8.4833,
     'Bitburg': 6.5256, 'Ratzeburg': 10.7567, 'Jever': 7.9008, 'Lübben (Spreewald)': 13.9, 'Montabaur': 7.8258,
     'Erbach': 8.9931, 'Lauterbach': 9.3944, 'Haßfurt': 10.5123, 'Eichstätt': 11.1839, 'Tauberbischofsheim': 9.6628,
     'Pfarrkirchen': 12.9443, 'Ebersberg': 11.9667, 'Perleberg': 11.8667, 'Bad Fallingbostel': 9.6967,
     'Hildburghausen': 10.7289, 'Miesbach': 11.8338, 'Bad Schwalbach': 8.0694, 'Regen': 13.1264, 'Eisenberg': 11.9,
     'Altötting': 12.6759, 'Bad Ems': 7.7106, 'Lüchow': 11.15, 'Miltenberg': 9.2644, 'Wunsiedel': 11.9994,
     'Herzberg': 13.2331, 'Plön': 10.4214, 'Schleiz': 11.8167, 'Tirschenreuth': 12.3369, 'Beeskow': 14.25,
     'Daun': 6.8319, 'Simmern': 7.5167, 'Kirchheimbolanden': 8.0117, 'Freyung': 13.5475, 'Birkenfeld': 7.1833,
     'Altenkirchen': 7.6456, 'Seelow': 14.3831, 'Kusel': 7.3981, 'Cochem': 7.1667, 'Offenbach': 8.7665,
     'Frankenthal': 8.3639, 'Weiden': 12.1561, 'Nienburg': 9.2208, 'Verden': 9.2349, 'Köthen': 11.9709,
     'Rotenburg': 9.4108, 'Karlstadt': 9.7724, 'Homberg': 9.4026, 'Garmisch-Partenkirchen': 11.0958}

    lat = {'Berlin': 52.5167, 'Hamburg': 53.55, 'Munich': 48.1372, 'Cologne': 50.9422, 'Frankfurt': 50.1136,
     'Bremen': 53.1153, 'Düsseldorf': 51.2311, 'Stuttgart': 48.7761, 'Leipzig': 51.3333, 'Dortmund': 51.5139,
     'Essen': 51.4508, 'Dresden': 51.05, 'Hannover': 52.3744, 'Nuremberg': 49.4539, 'Duisburg': 51.4322,
     'Bochum': 51.4833, 'Wuppertal': 51.2667, 'Bielefeld': 52.0167, 'Bonn': 50.7339, 'Münster': 51.9625,
     'Karlsruhe': 49.0167, 'Mannheim': 49.4878, 'Augsburg': 48.3717, 'Kassel': 51.3166, 'Wiesbaden': 50.0825,
     'Mönchengladbach': 51.2, 'Gelsenkirchen': 51.5167, 'Braunschweig': 52.2692, 'Aachen': 50.7762, 'Kiel': 54.3233,
     'Chemnitz': 50.8333, 'Halle': 51.4828, 'Magdeburg': 52.1278, 'Freiburg im Breisgau': 47.9947, 'Krefeld': 51.3333,
     'Mainz': 50.0, 'Lübeck': 53.8697, 'Oberhausen': 51.4699, 'Rostock': 54.0833, 'Erfurt': 50.9787, 'Hagen': 51.3594,
     'Potsdam': 52.4, 'Saarbrücken': 49.2333, 'Hamm': 51.6667, 'Ludwigshafen': 49.4811, 'Mülheim': 51.4275,
     'Oldenburg': 53.1439, 'Osnabrück': 52.2789, 'Leverkusen': 51.0333, 'Heidelberg': 49.4122, 'Solingen': 51.1667,
     'Darmstadt': 49.8667, 'Herne': 51.5426, 'Neuss': 51.2003, 'Regensburg': 49.0167, 'Paderborn': 51.7167,
     'Ingolstadt': 48.7636, 'Würzburg': 49.7944, 'Fürth': 49.4783, 'Ulm': 48.3984, 'Heilbronn': 49.1404,
     'Pforzheim': 48.895, 'Wolfsburg': 52.4231, 'Göttingen': 51.5339, 'Bottrop': 51.5232, 'Reutlingen': 48.4833,
     'Koblenz': 50.3597, 'Bremerhaven': 53.55, 'Recklinghausen': 51.6167, 'Bergisch Gladbach': 50.9917,
     'Erlangen': 49.5964, 'Jena': 50.9272, 'Remscheid': 51.1802, 'Trier': 49.7567, 'Salzgitter': 52.1503,
     'Moers': 51.4592, 'Siegen': 50.8756, 'Hildesheim': 52.15, 'Cottbus': 51.7606, 'Gütersloh': 51.9,
     'Kaiserslautern': 49.4447, 'Witten': 51.4333, 'Hanau': 50.1328, 'Schwerin': 53.6333, 'Gera': 50.8782,
     'Esslingen': 48.7406, 'Ludwigsburg': 48.8975, 'Iserlohn': 51.3833, 'Düren': 50.8, 'Tübingen': 48.52,
     'Flensburg': 54.7819, 'Zwickau': 50.7189, 'Gießen': 50.5833, 'Ratingen': 51.3, 'Lünen': 51.6167,
     'Villingen-Schwenningen': 48.0603, 'Konstanz': 47.6633, 'Marl': 51.6667, 'Worms': 49.6319, 'Velbert': 51.34,
     'Minden': 52.2883, 'Dessau-Roßlau': 51.8333, 'Neumünster': 54.0714, 'Norderstedt': 53.7064, 'Delmenhorst': 53.0506,
     'Bamberg': 49.8917, 'Viersen': 51.2556, 'Marburg': 50.8167, 'Rheine': 52.2833, 'Wilhelmshaven': 53.5167,
     'Gladbeck': 51.5713, 'Lüneburg': 53.2525, 'Troisdorf': 50.8161, 'Dorsten': 51.66, 'Bayreuth': 49.9481,
     'Detmold': 51.9378, 'Arnsberg': 51.3967, 'Castrop-Rauxel': 51.55, 'Lüdenscheid': 51.2198, 'Landshut': 48.5397,
     'Brandenburg': 52.4117, 'Bocholt': 51.8333, 'Aschaffenburg': 49.9757, 'Celle': 52.6256, 'Kempten': 47.7333,
     'Fulda': 50.5528, 'Aalen': 48.8372, 'Lippstadt': 51.6667, 'Dinslaken': 51.5667, 'Herford': 52.1146,
     'Kerpen': 50.8719, 'Rüsselsheim': 49.995, 'Weimar': 50.9833, 'Plauen': 50.495, 'Sindelfingen': 48.7133,
     'Neuwied': 50.4286, 'Dormagen': 51.0964, 'Neubrandenburg': 53.5569, 'Grevenbroich': 51.0883, 'Rosenheim': 47.8561,
     'Herten': 51.6, 'Bergheim': 50.9667, 'Schwäbisch Gmünd': 48.8, 'Friedrichshafen': 47.6542, 'Garbsen': 52.4183,
     'Wesel': 51.6586, 'Hürth': 50.8775, 'Offenburg': 48.4708, 'Stralsund': 54.3092, 'Greifswald': 54.0833,
     'Langenfeld': 51.1167, 'Neu-Ulm': 48.3833, 'Unna': 51.5347, 'Euskirchen': 50.6613, 'Frankfurt (Oder)': 52.35,
     'Göppingen': 48.7025, 'Hameln': 52.1031, 'Stolberg': 50.7667, 'Eschweiler': 50.8167, 'Görlitz': 51.1528,
     'Langenhagen': 52.4394, 'Meerbusch': 51.2667, 'Sankt Augustin': 50.77, 'Hilden': 51.1714, 'Waiblingen': 48.8303,
     'Baden-Baden': 48.7619, 'Hattingen': 51.3992, 'Lingen': 52.5233, 'Bad Homburg': 50.2292, 'Bad Salzuflen': 52.0875,
     'Pulheim': 51.0, 'Schweinfurt': 50.05, 'Nordhorn': 52.4333, 'Neustadt': 49.5656, 'Wetzlar': 50.5667,
     'Ahlen': 51.7633, 'Frechen': 50.9167, 'Passau': 48.5667, 'Wolfenbüttel': 52.1622, 'Ibbenbüren': 52.2778,
     'Kleve': 51.79, 'Bad Kreuznach': 49.8469, 'Goslar': 51.906, 'Gummersbach': 51.0333, 'Ravensburg': 47.7831,
     'Willich': 51.2631, 'Speyer': 49.3194, 'Böblingen': 48.6833, 'Peine': 52.3203, 'Emden': 53.3669,
     'Elmshorn': 53.7547, 'Erftstadt': 50.8167, 'Rastatt': 48.8572, 'Heidenheim': 48.6761, 'Lörrach': 47.6156,
     'Leonberg': 48.8014, 'Bergkamen': 51.6167, 'Bad Oeynhausen': 52.2, 'Freising': 48.4028,
     'Rheda-Wiedenbrück': 51.8417, 'Cuxhaven': 53.8667, 'Bornheim': 50.7592, 'Gronau': 52.2125, 'Straubing': 48.8772,
     'Stade': 53.6008, 'Soest': 51.5711, 'Dachau': 48.2603, 'Alsdorf': 50.8744, 'Landau': 49.1994, 'Dülmen': 51.8308,
     'Melle': 52.2031, 'Neunkirchen': 49.3448, 'Herzogenrath': 50.8667, 'Schwerte': 51.4458, 'Oberursel': 50.2028,
     'Wittenberg': 51.8671, 'Hof': 50.3167, 'Filderstadt': 48.6667, 'Gotha': 50.9489, 'Fellbach': 48.8086,
     'Bünde': 52.2, 'Albstadt': 48.2119, 'Weinheim': 49.5561, 'Rodgau': 50.0256, 'Bruchsal': 49.1333,
     'Oranienburg': 52.7544, 'Brühl': 50.8333, 'Erkrath': 51.2239, 'Neustadt am Rübenberge': 52.5055, 'Lehrte': 52.3725,
     'Kaufbeuren': 47.88, 'Falkensee': 52.5583, 'Memmingen': 47.9878, 'Kaarst': 51.2278, 'Erkelenz': 51.08,
     'Pinneberg': 53.6591, 'Bietigheim-Bissingen': 48.9667, 'Kamen': 51.5917, 'Wismar': 53.8925, 'Borken': 51.8439,
     'Gifhorn': 52.4886, 'Nettetal': 51.3167, 'Eisenach': 50.9747, 'Dreieich': 50.0189, 'Aurich': 53.4714,
     'Amberg': 49.4444, 'Heinsberg': 51.0631, 'Ansbach': 49.3, 'Homburg': 49.3167, 'Nordhausen': 51.505,
     'Wunstorf': 52.4238, 'Seevetal': 53.3833, 'Siegburg': 50.8014, 'Laatzen': 52.3077, 'Germering': 48.1333,
     'Coburg': 50.2585, 'Königswinter': 50.6833, 'Nürtingen': 48.6267, 'Freiberg': 50.9119, 'Schwabach': 49.3292,
     'Lemgo': 52.0277, 'Kirchheim unter Teck': 48.6483, 'Bensheim': 49.6811, 'Schwäbisch Hall': 49.1122,
     'Weißenfels': 51.2, 'Pirmasens': 49.2, 'Eberswalde': 52.8331, 'Halberstadt': 51.8958, 'Buxtehude': 53.4769,
     'Leinfelden-Echterdingen': 48.6928, 'Neumarkt': 49.2833, 'Hückelhoven': 51.0608, 'Hofheim': 50.0876, 'Löhne': 52.2,
     'Schorndorf': 48.8, 'Stendal': 52.6, 'Freital': 51.0167, 'Völklingen': 49.25, 'Ettlingen': 48.9333,
     'Ostfildern': 48.7228, 'Maintal': 50.15, 'Buchholz in der Nordheide': 53.3285, 'Ahaus': 52.0794,
     'Ilmenau': 50.6872, 'Mettmann': 51.25, 'Würselen': 50.8247, 'Bitterfeld': 51.6167, 'Bautzen': 51.1814,
     'Pirna': 50.9622, 'Niederkassel': 50.8167, 'Langen': 49.9893, 'Greven': 52.0917, 'Fürstenfeldbruck': 48.1778,
     'Neu Isenburg': 50.0558, 'Papenburg': 53.0667, 'Kamp-Lintfort': 51.5, 'Backnang': 48.9464, 'Warendorf': 51.9539,
     'Königs Wusterhausen': 52.2917, 'Suhl': 50.6106, 'Beckum': 51.7558, 'Erding': 48.3001, 'Coesfeld': 51.9458,
     'Mühlhausen': 51.2167, 'Wesseling': 50.8207, 'Kehl': 48.5711, 'Emsdetten': 52.1728, 'Tuttlingen': 47.985,
     'Sankt Ingbert': 49.3, 'Porta Westfalica': 52.2167, 'Sinsheim': 49.25, 'Meppen': 52.6906, 'Limburg': 50.3833,
     'Ingelheim': 49.9747, 'Lage': 51.9833, 'Cloppenburg': 52.8478, 'Winsen': 53.3667, 'Mörfelden-Walldorf': 49.9896,
     'Wermelskirchen': 51.1392, 'Datteln': 51.6539, 'Kempen': 51.3658, 'Saarlouis': 49.3167, 'Leer': 53.2308,
     'Seelze': 52.3961, 'Crailsheim': 49.1347, 'Barsinghausen': 52.3031, 'Balingen': 48.2731, 'Zweibrücken': 49.2494,
     'Viernheim': 49.538, 'Steinfurt': 52.1504, 'Merseburg': 51.3544, 'Hemer': 51.3833, 'Dietzenbach': 50.0086,
     'Radebeul': 51.1033, 'Bad Vilbel': 50.1781, 'Geldern': 51.5197, 'Goch': 51.6839, 'Kornwestheim': 48.8597,
     'Stuhr': 53.0167, 'Uelzen': 52.9647, 'Deggendorf': 48.8353, 'Wedel': 53.5833, 'Ahrensburg': 53.6747,
     'Rheinfelden (Baden)': 47.5611, 'Korschenbroich': 51.1833, 'Biberach': 48.0981, 'Wernigerode': 51.835,
     'Bernburg': 51.8, 'Jülich': 50.9222, 'Lampertheim': 49.5942, 'Vechta': 52.7306, 'Naumburg': 51.1521,
     'Forchheim': 49.7197, 'Bad Nauheim': 50.3667, 'Hoyerswerda': 51.4331, 'Altenburg': 50.985, 'Delbrück': 51.7667,
     'Fürstenwalde': 52.3667, 'Achim': 53.013, 'Itzehoe': 53.925, 'Georgsmarienhütte': 52.2031, 'Herrenberg': 48.5967,
     'Oer-Erkenschwick': 51.6422, 'Radolfzell am Bodensee': 47.7369, 'Kreuztal': 50.962, 'Ganderkesee': 53.0358,
     'Rheinberg': 51.5467, 'Bramsche': 52.4089, 'Neuruppin': 52.9222, 'Einbeck': 51.8167, 'Werl': 51.5528,
     'Schönebeck': 52.0167, 'Burgdorf': 52.4438, 'Gevelsberg': 51.3265, 'Weyhe': 52.9936, 'Geesthacht': 53.4375,
     'Haan': 51.1931, 'Lohmar': 50.8415, 'Osterholz-Scharmbeck': 53.2269, 'Weil am Rhein': 47.5947,
     'Ennepetal': 51.3021, 'Riesa': 51.3081, 'Taunusstein': 50.1435, 'Andernach': 50.4397, 'Meschede': 51.3503,
     'Schwedt (Oder)': 53.0631, 'Friedberg': 50.3353, 'Bad Hersfeld': 50.8683, 'Gaggenau': 48.8039, 'Merzig': 49.4471,
     'Neuburg': 48.7333, 'Werve': 51.6667, 'Vaihingen an der Enz': 48.9328, 'Rietberg': 51.8, 'Saalfeld': 50.6506,
     'Bretten': 49.0364, 'Waltrop': 51.6236, 'Oelde': 51.8258, 'Tönisvorst': 51.3208, 'Güstrow': 53.7939,
     'Landsberg': 48.0528, 'Northeim': 51.7067, 'Kelkheim (Taunus)': 50.138, 'Springe': 52.2167,
     'Unterschleißheim': 48.2833, 'Bühl': 48.6953, 'Schwandorf': 49.3236, 'Höxter': 51.7667, 'Rösrath': 50.9,
     'Bad Zwischenahn': 53.1836, 'Schwelm': 51.2904, 'Rendsburg': 54.3044, 'Winnenden': 48.8764,
     'Bad Neuenahr-Ahrweiler': 50.5447, 'Grimma': 51.2386, 'Wegberg': 51.1333, 'Geislingen an der Steige': 48.6244,
     'Königsbrunn': 48.2689, 'Henstedt-Ulzburg': 53.7833, 'Meißen': 51.1636, 'Leichlingen': 51.1167,
     'Kevelaer': 51.5833, 'Zeitz': 51.0478, 'Emmendingen': 48.1214, 'Blankenfelde': 52.35, 'Sundern': 51.3167,
     'Baunatal': 51.2589, 'Reinbek': 53.5089, 'Mechernich': 50.6, 'Hattersheim': 50.0697, 'Wetter (Ruhr)': 51.3881,
     'Griesheim': 49.8594, 'Arnstadt': 50.8342, 'Aschersleben': 51.75, 'Geilenkirchen': 50.9653, 'Rheinbach': 50.6256,
     'Overath': 50.9328, 'Baesweiler': 50.9, 'Leimen': 49.3481, 'Wangen im Allgäu': 47.6858,
     'Schloß Holte-Stukenbrock': 51.8833, 'Lohne': 52.6667, 'Wiesloch': 49.2942, 'Hamminkeln': 51.7319,
     'Strausberg': 52.5808, 'Lauf': 49.5103, 'Neckarsulm': 49.1917, 'Heiligenhaus': 51.3265, 'Sangerhausen': 51.4667,
     'Hennigsdorf': 52.6378, 'Ehingen an der Donau': 48.2833, 'Butzbach': 50.4367, 'Nordenham': 53.5,
     'Hohen Neuendorf': 52.6667, 'Ludwigsfelde': 52.2997, 'Mühlacker': 48.95, 'Heppenheim': 49.6415, 'Selm': 51.6833,
     'Weiterstadt': 49.9, 'Pfaffenhofen': 48.5333, 'Kulmbach': 50.1, 'Sankt Wendel': 49.4667, 'Teltow': 52.4022,
     'Bad Honnef am Rhein': 50.645, 'Helmstedt': 52.2281, 'Bingen am Rhein': 49.9669, 'Achern': 48.6314,
     'Zirndorf': 49.45, 'Roth': 49.2461, 'Lennestadt': 51.1236, 'Verl': 51.8831, 'Lindau': 47.5458, 'Lübbecke': 52.3081,
     'Rinteln': 52.1906, 'Brilon': 51.3956, 'Staßfurt': 51.8667, 'Zittau': 50.8961, 'Plettenberg': 51.2128,
     'Groß-Gerau': 49.9214, 'Schleswig': 54.5153, 'Geretsried': 47.8667, 'Rottweil': 48.1681, 'Friedrichsdorf': 50.2569,
     'Petershagen': 52.3833, 'Pfungstadt': 49.8056, 'Bad Oldesloe': 53.8117, 'Olpe': 51.0289, 'Rathenow': 52.6,
     'Senftenberg': 51.5167, 'Meiningen': 50.55, 'Waldshut-Tiengen': 47.6231, 'Sonneberg': 50.35, 'Salzwedel': 52.85,
     'Calw': 48.7144, 'Korbach': 51.2719, 'Starnberg': 47.9972, 'Freudenstadt': 48.4633, 'Mosbach': 49.35,
     'Husum': 54.4769, 'Westerstede': 53.25, 'Burg': 52.2725, 'Weilheim': 47.8333, 'Bad Kissingen': 50.2,
     'Stadthagen': 52.3247, 'Apolda': 51.0247, 'Heide': 54.1961, 'Osterode': 51.7286, 'Kitzingen': 49.7333,
     'Sonthofen': 47.5142, 'Sondershausen': 51.3667, 'Aichach': 48.45, 'Germersheim': 49.2167, 'Günzburg': 48.4525,
     'Luckenwalde': 52.0831, 'Traunstein': 47.8683, 'Mühldorf': 48.2456, 'Wittmund': 53.5747, 'Bad Salzungen': 50.8117,
     'Lichtenfels': 50.1333, 'Greiz': 50.6547, 'Donauwörth': 48.7184, 'Torgau': 51.5603, 'Holzminden': 51.8297,
     'Dingolfing': 48.6333, 'Annaberg-Buchholz': 50.58, 'Eschwege': 51.1881, 'Haldensleben': 52.2833,
     'Sömmerda': 51.1617, 'Prenzlau': 53.3167, 'Wittlich': 49.9869, 'Dillingen': 48.5667, 'Bad Tölz': 47.7603,
     'Marktoberdorf': 47.7667, 'Alzey': 49.7517, 'Bad Dürkheim': 49.4618, 'Weißenburg': 49.0306,
     'Bad Reichenhall': 47.7247, 'Forst (Lausitz)': 51.7333, 'Parchim': 53.4167, 'Sigmaringen': 48.0867,
     'Bad Segeberg': 53.9356, 'Heilbad Heiligenstadt': 51.3775, 'Eutin': 54.1378, 'Cham': 49.2183, 'Diepholz': 52.6072,
     'Kronach': 50.2411, 'Kelheim': 48.9167, 'Bad Neustadt': 50.3219, 'Künzelsau': 49.2833, 'Mindelheim': 48.0333,
     'Brake': 53.3333, 'Bitburg': 49.9747, 'Ratzeburg': 53.7017, 'Jever': 53.5744, 'Lübben (Spreewald)': 51.95,
     'Montabaur': 50.4375, 'Erbach': 49.6569, 'Lauterbach': 50.6378, 'Haßfurt': 50.0353, 'Eichstätt': 48.8919,
     'Tauberbischofsheim': 49.6225, 'Pfarrkirchen': 48.4419, 'Ebersberg': 48.0833, 'Perleberg': 53.0667,
     'Bad Fallingbostel': 52.8675, 'Hildburghausen': 50.4261, 'Miesbach': 47.789, 'Bad Schwalbach': 50.1401,
     'Regen': 48.97, 'Eisenberg': 50.9667, 'Altötting': 48.2264, 'Bad Ems': 50.3381, 'Lüchow': 52.9667,
     'Miltenberg': 49.7039, 'Wunsiedel': 50.0374, 'Herzberg': 51.6831, 'Plön': 54.1622, 'Schleiz': 50.5833,
     'Tirschenreuth': 49.8789, 'Beeskow': 52.1667, 'Daun': 50.1986, 'Simmern': 49.9833, 'Kirchheimbolanden': 49.6664,
     'Freyung': 48.8075, 'Birkenfeld': 49.65, 'Altenkirchen': 50.6872, 'Seelow': 52.5167, 'Kusel': 49.5347,
     'Cochem': 50.1469, 'Offenbach': 50.1006, 'Frankenthal': 49.5436, 'Weiden': 49.6768, 'Nienburg': 52.6461,
     'Verden': 52.9234, 'Köthen': 51.7518, 'Rotenburg': 53.1113, 'Karlstadt': 49.9603, 'Homberg': 51.0299,
     'Garmisch-Partenkirchen': 47.4921}
